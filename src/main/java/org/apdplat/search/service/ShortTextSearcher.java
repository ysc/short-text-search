package org.apdplat.search.service;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import org.apache.commons.lang3.StringUtils;
import org.apdplat.search.model.Document;
import org.apdplat.search.model.Query;
import org.apdplat.search.model.SearchResult;
import org.apdplat.search.utils.ConcurrentLRUCache;
import org.apdplat.search.utils.ConfUtils;
import org.apdplat.search.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 自定制的精准短文本搜索服务
 * Created by ysc on 1/8/17.
 */
public class ShortTextSearcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShortTextSearcher.class);

    private AtomicLong currentProcessingSearchCount =new AtomicLong();
    private static final int SEARCH_MAX_CONCURRENT = ConfUtils.getInt("search.max.concurrent", 1000);

    private Map<String, AtomicInteger> searchHistories = new ConcurrentHashMap<>();
    private Map<String, AtomicInteger> searchCountPerDay = new ConcurrentHashMap<>();

    private AtomicInteger indexIdGenerator = new AtomicInteger();
    private Map<String, Set<Integer>> INVERTED_INDEX = new ConcurrentHashMap<>();
    private Map<Integer, Integer> INDEX_TO_DOCUMENT = new ConcurrentHashMap<>();
    private Map<Integer, Integer> DOCUMENT_TO_INDEX = new ConcurrentHashMap<>();
    private Map<Integer, Document> DOCUMENT = new ConcurrentHashMap<>();
    private AtomicLong indexTotalCost =new AtomicLong();

    private Set<String> charPinYin = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private int charMaxPinYinLength = 0;

    private static final String PRE = ConfUtils.get("highlight.prefix", "<font color='red'>");
    private static final String SUF = ConfUtils.get("highlight.suffix", "</font>");
    private static final int SEARCH_WORD_LENGTH_LIMIT = ConfUtils.getInt("search.word.length.limit", 30);
    private static final int TOPN_LENGTH_LIMIT = ConfUtils.getInt("topn.length.limit", 1000);
    private AtomicInteger searchCount = new AtomicInteger();
    private AtomicLong maxSearchTime = new AtomicLong();
    private AtomicLong totalSearchTime = new AtomicLong();
    private long searchServiceStartupTime = System.currentTimeMillis();

    private int maxNgram = 6;

    private boolean cacheEnabled = ConfUtils.getBoolean("cache.enabled", true);
    private ConcurrentLRUCache<String, SearchResult> cache = null;

    public ShortTextSearcher(int maxNgram){
        this(maxNgram, true);
    }

    public ShortTextSearcher(int maxNgram, boolean cacheEnabled){
        this.cacheEnabled = cacheEnabled;
        if(cacheEnabled) {
            cache = new ConcurrentLRUCache<>(ConfUtils.getInt("max.cache.size", 1000));
        }
        if(maxNgram > 1 && maxNgram <= 6) {
            this.maxNgram = maxNgram;
        }else{
            LOGGER.error("指定的参数maxNgram: {} 超出范围(1,6], 使用默认值: {}", maxNgram, this.maxNgram);
        }
        LOGGER.info("maxNgram: {}", this.maxNgram);
        LOGGER.info("搜索词长度限制: {}", SEARCH_WORD_LENGTH_LIMIT);
        LOGGER.info("topN长度限制: {}", TOPN_LENGTH_LIMIT);
    }

    public void clear(){
        if(cacheEnabled) {
            cache.clear();
        }
        INVERTED_INDEX.keySet().forEach(k->INVERTED_INDEX.get(k).clear());
        INVERTED_INDEX.clear();
        INDEX_TO_DOCUMENT.clear();
        DOCUMENT_TO_INDEX.clear();
        DOCUMENT.clear();
        charPinYin.clear();
    }

    public void saveIndex(){
        long start = System.currentTimeMillis();
        saveInvertIndex(INVERTED_INDEX);
        saveIndexIdDocumentIdMapping(INDEX_TO_DOCUMENT);
        saveDocument(DOCUMENT);
        LOGGER.info("保存索引耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    public void loadIndex(String zipFile){
        long start = System.currentTimeMillis();
        try (FileSystem fs = FileSystems.newFileSystem(Paths.get(zipFile), ShortTextSearcher.class.getClassLoader())) {
            for(Path path : fs.getRootDirectories()){
                LOGGER.info("处理目录："+path);
                Files.walkFileTree(path, new SimpleFileVisitor<Path>(){

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        LOGGER.info("处理文件："+file);
                        // 拷贝到本地文件系统
                        //Path temp = Paths.get("target/"+file);
                        //Files.copy(file, temp, StandardCopyOption.REPLACE_EXISTING);
                        switch (file.toString()){
                            case "/invert_index.txt":
                                loadInvertIndex(file);
                                break;
                            case "/index_id_to_document_id.txt":
                                loadIndexIdDocumentIdMapping(file);
                                break;
                            case "/document.txt":
                                loadDocument(file);
                                break;
                        }
                        return FileVisitResult.CONTINUE;
                    }

                });
            }
        }catch (Exception e){
            LOGGER.error("加载索引文件出错! "+zipFile, e);
        }
        LOGGER.info("加载索引耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    private void saveDocument(Map<Integer, Document> documentMap){
        try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("document.txt")))){
            documentMap.keySet().stream().sorted().forEach(documentId->{
                Document document = documentMap.get(documentId);
                try{
                    StringBuilder doc = new StringBuilder();
                    doc.append(document.getValue());
                    bufferedWriter.write(documentId+"="+doc.toString()+"\n");
                } catch (IOException e) {
                    LOGGER.error("save document failed", e);
                    LOGGER.error("document: {}", document);
                }
            });
        }catch (Exception e){
            LOGGER.error("save document failed", e);
        }
    }
    private void loadDocument(Path file){
        try (Stream<String> stream = Files.lines(file)) {
            stream.forEach(line->{
                try{
                    String[] attr = line.split("=");
                    if(attr == null || attr.length < 2){
                        LOGGER.info("document corrupted! {}", line);
                        return;
                    }
                    StringBuilder value = new StringBuilder();
                    for(int i=1; i<attr.length; i++){
                        value.append(attr[i]);
                    }
                    int documentId = Integer.parseInt(attr[0]);
                    String documentValue = value.toString();
                    if(StringUtils.isBlank(documentValue)){
                        LOGGER.info("document value corrupted! {}", line);
                        return;
                    }
                    Document document = new Document();
                    document.setId(documentId);
                    document.setValue(documentValue);
                    DOCUMENT.put(documentId, document);
                }catch (Exception e){
                    LOGGER.error("load document failed! "+line, e);
                }
            });
        }catch (Exception e){
            LOGGER.error("load document failed! "+file, e);
        }
    }
    private void saveIndexIdDocumentIdMapping(Map<Integer, Integer> indexIdDocumentIdMapping){
        try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("index_id_to_document_id.txt")))){
            indexIdDocumentIdMapping.keySet().stream().sorted().forEach(indexId->{
                try {
                    bufferedWriter.write(indexId+"="+indexIdDocumentIdMapping.get(indexId)+"\n");
                } catch (IOException e) {
                    LOGGER.error("save indexId documentId mapping failed", e);
                    LOGGER.info("indexId: {}", indexId);
                }
            });
        }catch (Exception e){
            LOGGER.error("save indexId documentId mapping failed", e);
        }
    }
    private void loadIndexIdDocumentIdMapping(Path file){
        try (Stream<String> stream = Files.lines(file)) {
            stream.forEach(line->{
                try{
                    String[] attr = line.split("=");
                    if(attr == null || attr.length != 2){
                        LOGGER.info("indexId documentId mapping format corrupted! {}", line);
                        return;
                    }
                    int indexId = Integer.parseInt(attr[0]);
                    int documentId = Integer.parseInt(attr[1]);
                    INDEX_TO_DOCUMENT.put(indexId, documentId);
                    DOCUMENT_TO_INDEX.put(documentId, indexId);
                }catch (Exception e){
                    LOGGER.error("load indexId documentId mapping failed! "+line, e);
                }
            });
        }catch (Exception e){
            LOGGER.error("load indexId documentId mapping failed! "+file, e);
        }
    }
    private void saveInvertIndex(Map<String, Set<Integer>> invertIndex){
        try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("invert_index.txt")));
            BufferedWriter bufferedWriter2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("invert_index_length_status.txt")))){
            invertIndex.entrySet().stream().sorted((a,b)->b.getValue().size()-a.getValue().size()).forEach(e->{
                try {
                    StringBuilder ids = new StringBuilder();
                    e.getValue().stream().sorted().forEach(id->ids.append(id).append(" "));
                    bufferedWriter.write( e.getKey()+"="+ids.toString()+"\n");
                    bufferedWriter2.write(e.getKey()+"="+e.getValue().size()+"\n");
                } catch (Exception ex) {
                    LOGGER.error("save invert index failed", ex);
                    LOGGER.info("term: {}, idsLength: {}, ids: {}", e.getKey(), e.getValue().size(), e.getValue());
                }
            });
        }catch (Exception e){
            LOGGER.error("save invert index failed", e);
        }
    }
    private void loadInvertIndex(Path file){
        try (Stream<String> stream = Files.lines(file)) {
            stream.forEach(line->{
                try{
                    String[] attr = line.split("=");
                    if(attr == null || attr.length != 2){
                        LOGGER.info("invert index format corrupted! {}", line);
                        return;
                    }
                    String term = attr[0];
                    if(StringUtils.isBlank(term)){
                        LOGGER.info("invert index term is blank! {}", line);
                        return;
                    }
                    String[] indexIds = attr[1].split("\\s+");
                    if(indexIds == null || indexIds.length < 1){
                        LOGGER.info("invert index ids is empty! {}", line);
                        return;
                    }
                    Set<Integer> ids = Arrays.asList(indexIds).stream().map(indexId->Integer.parseInt(indexId)).collect(Collectors.toSet());
                    if(ids == null || ids.size() != indexIds.length){
                        LOGGER.info("invert index ids is not complete! {}", line);
                        return;
                    }
                    INVERTED_INDEX.putIfAbsent(term, Collections.newSetFromMap(new ConcurrentHashMap<>()));
                    INVERTED_INDEX.get(term).addAll(ids);
                }catch (Exception e){
                    LOGGER.error("load invert index failed! "+line, e);
                }
            });
        }catch (Exception e){
            LOGGER.error("load invert index failed! "+file, e);
        }
    }

    public String getKeyAndHitCount(){
        return cache.getKeyAndHitCount();
    }

    public String getCacheStatus(){
        return cache.getStatus();
    }

    public String getSearchStatus(){
        StringBuilder status = new StringBuilder();
        status.append("系统持续运行时间: ")
                .append(TimeUtils.getTimeDes(System.currentTimeMillis()-searchServiceStartupTime))
                .append("\n")
                .append("已搜索次数: ")
                .append(searchCount.get())
                .append("\n")
                .append("最慢搜索组件执行时间: ")
                .append(TimeUtils.getTimeDes(maxSearchTime.get()))
                .append("\n")
                .append("累计搜索时间: ")
                .append(TimeUtils.getTimeDes(totalSearchTime.get()))
                .append("\n")
                .append("搜索时间占比: ")
                .append(totalSearchTime.get()/(float)(System.currentTimeMillis()-searchServiceStartupTime)*100)
                .append("%\n")
                .append("搜索词长度限制: ")
                .append(SEARCH_WORD_LENGTH_LIMIT)
                .append("\n")
                .append("topN长度限制: ")
                .append(TOPN_LENGTH_LIMIT)
                .append("\n");
        status.append("每日搜索次数统计: \n");
        searchCountPerDay.keySet().stream().sorted((a, b)->b.compareTo(a)).forEach(day->{
            status.append("\t").append(day).append(" --> ").append(searchCountPerDay.get(day)).append("\n");
        });
        return status.toString();
    }
    public String getIndexStatus(){
        StringBuilder status = new StringBuilder();
        status.append("索引文档数: ")
                .append(DOCUMENT.size())
                .append("\n")
                .append("索引耗时: ")
                .append(TimeUtils.getTimeDes(indexTotalCost.get()))
                .append("\n")
                .append("TERM数: ")
                .append(INVERTED_INDEX.size())
                .append("\n")
                .append("最长拼音字母数: ")
                .append(charMaxPinYinLength)
                .append("\n")
                .append("索引Ngram最大长度: ")
                .append(maxNgram)
                .append("\n");
        return status.toString();
    }

    public void clearSearchHistories(){
        searchHistories.clear();
    }

    public String getSearchHistories(){
        StringBuilder result = new StringBuilder();
        AtomicInteger i = new AtomicInteger();
        searchHistories.entrySet()
                .stream()
                .sorted((a,b)->{
                    int q = b.getValue().get()-a.getValue().get();
                    if(q == 0){
                        q = a.getKey().compareTo(b.getKey());
                    }
                    return q;
                })
                .forEach(e->
                    result.append(i.incrementAndGet())
                            .append("\t")
                            .append(e.getKey())
                            .append("\t")
                            .append(e.getValue().get())
                            .append("\n")
                );
        return result.toString();
    }

    public int getMaxNgram(){
        return maxNgram;
    }

    public static int getSearchWordLengthLimit() {
        return SEARCH_WORD_LENGTH_LIMIT;
    }

    public static int getTopNLengthLimit() {
        return TOPN_LENGTH_LIMIT;
    }

    public String explain(String keyWords, int documentId){
        long start = System.currentTimeMillis();
        Query query = parse(keyWords);
        LOGGER.info("查询解析耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
        if(query.isEmpty()){
            return "查询解析后为空";
        }
        LOGGER.info("查询结构: {}", query.getKeyWordTerms());

        Document document = DOCUMENT.get(documentId);
        if(document == null){
            return "没有ID为 "+documentId+" 的文档";
        }
        LOGGER.info("解释搜索关键词: {} 和文档: {} 的评分公式", keyWords, document.getValue());
        StringBuilder explain = new StringBuilder();
        explain.append("搜索词[").append(keyWords).append("]切分如下:\n");
        int i = 1;
        for(String item : query.getKeyWordTerms()){
            explain.append("\t").append(i++).append(". ").append(item).append("\n");
        }
        String value = document.getValue();
        List<String> documentTerms = value.length() == 1 ? Arrays.asList(value, getAcronymPinYin(value), getFullPinYin(value)) : tokenize(value);
        explain.append("\n命中文档[").append(document.getValue()).append("]切分如下:\n");
        i = 1;
        for(String item : documentTerms){
            explain.append("\t").append(i++).append(". ").append(item).append("\n");
        }
        explain.append("\n评分过程如下:\n");
        int score = 0;
        List<Integer> sections = new ArrayList<>();
        i = 1;
        if(documentTerms.size() < query.getKeyWordTerms().size()){
            for(String documentTerm : documentTerms){
                if(query.getKeyWordTerms().contains(documentTerm)){
                    score += documentTerm.length();
                    sections.add(documentTerm.length());
                    explain.append("\tscorer ").append(i++).append(". ").append(documentTerm).append(" --> ").append(documentTerm.length()).append("\n");
                }
            }
        }else{
            for(String keywordTerm : query.getKeyWordTerms()){
                if(documentTerms.contains(keywordTerm)){
                    score += keywordTerm.length();
                    sections.add(keywordTerm.length());
                    explain.append("\tscorer ").append(i++).append(". ").append(keywordTerm).append(" --> ").append(keywordTerm.length()).append("\n");
                }
            }
        }
        keyWords = keyWords.trim().toLowerCase();
        if(keyWords.equals(document.getValue().trim().toLowerCase())
                || normalize(keyWords).equals(normalize(document.getValue()))){
            score += document.getValue().length();
            sections.add(document.getValue().length());
            explain.append("\tscorer ").append(i++).append(". exactly the same --> ").append(document.getValue().length()).append("\n");
        }
        boolean notContainChinese = notContainChinese(keyWords);
        if(notContainChinese && !query.hasNgramPinYin()){
            int delta = keyWords.length()-document.getValue().length();
            if(delta != 0){
                score += delta;
                sections.add(delta);
                explain.append("\tscorer ").append(i++).append(". just for non-chinese search --> ").append(keyWords).append(" and ").append(document.getValue()).append(" 's length delta: ").append(" ").append(keyWords.length()).append("-").append(document.getValue().length()).append(" = ").append(delta).append("\n");
            }
        }
        if(notContainChinese){
            String chineseValue = extractChinese(document.getValue());
            String acronym = getAcronymPinYin(chineseValue);
            String full = getFullPinYin(chineseValue);
            if(keyWords.equals(acronym)) {
                score += keyWords.length();
                sections.add(keyWords.length());
                explain.append("\tscorer ").append(i++).append(". just for pinyin search --> ").append(keyWords).append(" == ").append(acronym).append(" ").append(keyWords.length()).append("\n");
            }
            if(keyWords.equals(full)) {
                score += keyWords.length();
                sections.add(keyWords.length());
                explain.append("\tscorer ").append(i++).append(". just for pinyin search --> ").append(keyWords).append(" == ").append(full).append(" ").append(keyWords.length()).append("\n");
            }
        }
        explain.append("\n\t").append("total score: ").append(score).append(" <-- ").append(sections).append("\n\n");
        return explain.toString();
    }

    public SearchResult search(String keyWords, int topN){
        return search(keyWords, topN, true);
    }

    public String normalize(String text){
        StringBuilder normal = new StringBuilder();
        for(char c : text.toCharArray()){
            if(isEnglish(c)
                    || isNumber(c)
                    || isChinese(c)){
                normal.append(c);
            }
        }
        if(text.length() != normal.length()){
            if(LOGGER.isDebugEnabled()){
                LOGGER.debug("移除非英语数字和中文字符, 移除之前: {}, 移除之后: {}", text, normal.toString());
            }
        }
        return normal.toString().toLowerCase();
    }

    public boolean isNumber(char c){
        //大部分字符在这个范围
        if(c > '9' && c < '０'){
            return false;
        }
        if(c < '0'){
            return false;
        }
        if(c > '９'){
            return false;
        }
        return true;
    }

    public boolean isEnglishOrNumber(String text){
        boolean englishOrNumber = true;
        for(char c : text.toCharArray()){
            if(!isEnglish(c) && !isNumber(c)){
                englishOrNumber = false;
            }
        }
        return englishOrNumber;
    }

    /**
     * 全部字母大写代表首字母缩略词搜索模式
     * 全拼不同字之间可用空格分隔
     * @param keyWords
     * @return
     */
    public Query parse(String keyWords){
        Query query = new Query();
        if(StringUtils.isBlank(keyWords)){
            return query;
        }
        keyWords = keyWords.trim();
        boolean isAllUpperCase = false;
        if(notContainChinese(keyWords)){
            isAllUpperCase = StringUtils.isAllUpperCase(keyWords);
        }
        keyWords = keyWords.toLowerCase();
        if(keyWords.length() > SEARCH_WORD_LENGTH_LIMIT){
            String temp = keyWords;
            keyWords = keyWords.substring(0, SEARCH_WORD_LENGTH_LIMIT);
            LOGGER.warn("搜索词长度大于: {}, 将搜索词: {} 截短为: {}", SEARCH_WORD_LENGTH_LIMIT, temp, keyWords);
        }
        if(notContainChinese(keyWords) && ( keyWords.contains(" ") || keyWords.contains("'") ) ){
            List<String> terms = new ArrayList<>();
            if(keyWords.contains("'")){
                for(String term : keyWords.split("'")){
                    terms.add(term);
                }
            }else{
                for(String term : keyWords.split("\\s+")){
                    terms.add(term);
                }
            }
            query.addKeyWordTerms(ngramPinYin(terms));
            return query;
        }
        if(keyWords.length() < 1){
            return query;
        }
        if(notContainChinese(keyWords)) {
            query.addKeyWordTerm(keyWords);
            List<List<String>> allPinYin = factorizeAllPinYin(keyWords);
            if (!allPinYin.isEmpty()) {
                LOGGER.info("对搜索关键词: {} 进行拼音还原: {}", keyWords, allPinYin);
                List<String> ngramPinYin = ngramAllPinYin(allPinYin);
                if (!ngramPinYin.isEmpty()) {
                    query.hasNgramPinYin(true);
                    ngramPinYin.forEach(item -> query.addKeyWordTerm(item));
                    LOGGER.info("对还原后的拼音执行NGRAM: {}", ngramPinYin);
                }
            }
        }
        if(!query.hasNgramPinYin() || isAllUpperCase) {
            List<String> keyWordTerms = tokenize(keyWords, false);
            query.addKeyWordTerms(keyWordTerms);
        }
        return query;
    }

    public Logger getLogger(){
        return LOGGER;
    }
    public SearchResult search(String keyWords, int topN, boolean highlight){
        if(searchHistories.size() > 1000){
            searchHistories.clear();
        }
        searchHistories.putIfAbsent(keyWords, new AtomicInteger());
        searchHistories.get(keyWords).incrementAndGet();

        String key = TimeUtils.toString(System.currentTimeMillis(), "yyyyMMdd");
        searchCountPerDay.putIfAbsent(key, new AtomicInteger());
        searchCountPerDay.get(key).incrementAndGet();

        String identity = searchCount.incrementAndGet() + "-" + SEARCH_MAX_CONCURRENT;

        // control the request count
        if(currentProcessingSearchCount.incrementAndGet() > SEARCH_MAX_CONCURRENT){
            SearchResult searchResult = new SearchResult();
            searchResult.setOverload(true);
            LOGGER.info("优雅降级, 当前并发请求数量: {} 超过系统预设能承受的负载: {} {}", currentProcessingSearchCount.get(), SEARCH_MAX_CONCURRENT, identity);
            currentProcessingSearchCount.decrementAndGet();
            return searchResult;
        }

        // cache
        String cacheKey = keyWords+"_"+topN+"_"+highlight;
        if(cacheEnabled){
            SearchResult v = cache.get(cacheKey);
            if(v != null){
                LOGGER.info("搜索命中缓存: {}, topN: {},  highlight: {} {}", keyWords, topN, highlight, identity);
				currentProcessingSearchCount.decrementAndGet();
                return v;
            }
        }

        SearchResult searchResult = new SearchResult();
        searchResult.setIdentity(identity);
        if(topN > TOPN_LENGTH_LIMIT){
            LOGGER.warn("topN: {} 大于 {}, 限制为 {} {}", topN, TOPN_LENGTH_LIMIT, TOPN_LENGTH_LIMIT, identity);
            topN = TOPN_LENGTH_LIMIT;
        }
        LOGGER.info("搜索关键词: {}, topN: {},  highlight: {} {}", keyWords, topN, highlight, identity);
        long start = System.currentTimeMillis();
        Query query = parse(keyWords);
        long cost = System.currentTimeMillis()-start;
        totalSearchTime.addAndGet(cost);
        if(maxSearchTime.get() < cost){
            maxSearchTime.set(cost);
        }
        LOGGER.info("{} 查询解析耗时: {} {} ", cost, TimeUtils.getTimeDes(cost), identity);
        if(query.isEmpty()){
            currentProcessingSearchCount.decrementAndGet();
            return searchResult;
        }
        LOGGER.info("查询结构: {} {}", query.getKeyWordTerms(), identity);

        start = System.currentTimeMillis();
        Map<Integer, AtomicInteger> hits = new ConcurrentHashMap<>();
        // collect and init score doc
        query.getKeyWordTerms().parallelStream().forEach(keywordTerm -> {
            Set<Integer> indexIds = INVERTED_INDEX.get(keywordTerm);
            if(indexIds != null){
                Set<Integer> deletedIndexIds = new HashSet<Integer>();
                for(int indexId : indexIds){
                    Integer documentId = INDEX_TO_DOCUMENT.get(indexId);
                    if(documentId == null){
                        deletedIndexIds.add(indexId);
                        continue;
                    }
                    Document document = DOCUMENT.get(documentId);
                    if(document != null){
                        hits.putIfAbsent(documentId, new AtomicInteger());
                        hits.get(documentId).addAndGet(keywordTerm.length());
                    }else{
                        LOGGER.error("没有ID为: {} 的文档 {}", documentId, identity);
                    }
                }
                indexIds.removeAll(deletedIndexIds);
            }
        });
        int limitedDocCount = topN*10 < 1000 ? 1000 : topN*10;
        // limit doc
        Map<Integer, AtomicInteger> limitedDocuments = new ConcurrentHashMap<>();
        hits.entrySet()
                .parallelStream()
                .sorted((a,b)->b.getValue().intValue()-a.getValue().intValue())
                .limit(limitedDocCount)
                .forEach(e->limitedDocuments.put(e.getKey(), e.getValue()));
        cost = System.currentTimeMillis()-start;
        totalSearchTime.addAndGet(cost);
        if(maxSearchTime.get() < cost){
            maxSearchTime.set(cost);
        }
        LOGGER.info("{} 搜索耗时: {} {}", cost, TimeUtils.getTimeDes(cost), identity);
        LOGGER.info("搜索到的结果文档数: {}, 总的文档数: {}, 搜索结果占总文档的比例: {} %, 限制后的搜索结果数: {}, 限制后的搜索结果占总文档的比例: {} % {} ", hits.size(), DOCUMENT.size(), hits.size()/(float) DOCUMENT.size()*100, limitedDocuments.size(), limitedDocuments.size()/(float) DOCUMENT.size()*100, identity);
        start = System.currentTimeMillis();
        boolean notContainChinese = notContainChinese(keyWords);
        String finalKeyWords = keyWords.trim().toLowerCase();
        // final score doc
        Map<Integer, Integer> scores = new ConcurrentHashMap<>();
        limitedDocuments.entrySet().parallelStream().forEach(e->{
            int documentId = e.getKey();
            int score = e.getValue().get();
            Document doc = DOCUMENT.get(documentId);
            String value = doc.getValue();
            if(finalKeyWords.equals(value.trim().toLowerCase())
                    || normalize(finalKeyWords).equals(normalize(value))){
                score += value.length();
            }
            if(notContainChinese && !query.hasNgramPinYin()){
                int delta = finalKeyWords.length()-value.length();
                if(delta != 0){
                    score += delta;
                }
            }
            if(notContainChinese){
                String chineseValue = extractChinese(value);
                String acronym = getAcronymPinYin(chineseValue);
                String full = getFullPinYin(chineseValue);
                if(finalKeyWords.equals(acronym)) {
                    score += finalKeyWords.length();
                }
                if(finalKeyWords.equals(full)) {
                    score += finalKeyWords.length();
                }
            }
            scores.put(documentId, score);
        });
        cost = System.currentTimeMillis()-start;
        totalSearchTime.addAndGet(cost);
        if(maxSearchTime.get() < cost){
            maxSearchTime.set(cost);
        }
        LOGGER.info("{} 评分耗时: {} {}", cost, TimeUtils.getTimeDes(cost), identity);
        start = System.currentTimeMillis();
        // sort and limit doc
        List<Document> result = scores.entrySet().parallelStream().map(e->{
            Document doc = DOCUMENT.get(e.getKey()).clone();
            doc.setScore(e.getValue().intValue());
            return doc;
        }).sorted((a,b)->{
            int r = b.getScore()-a.getScore();
            if(r == 0){
                r = Long.valueOf(a.getId()).compareTo(Long.valueOf(b.getId()));
            }
            return r;
        }).limit(topN).collect(Collectors.toList());
        cost = System.currentTimeMillis()-start;
        totalSearchTime.addAndGet(cost);
        if(maxSearchTime.get() < cost){
            maxSearchTime.set(cost);
        }
        LOGGER.info("{} 排序耗时: {} {}", cost, TimeUtils.getTimeDes(cost), identity);
        if(highlight && !notContainChinese) {
            // highlight
            start = System.currentTimeMillis();
            highlight(result, keyWords, query.getKeyWordTerms());
            cost = System.currentTimeMillis()-start;
            totalSearchTime.addAndGet(cost);
            if(maxSearchTime.get() < cost){
                maxSearchTime.set(cost);
            }
            LOGGER.info("{} 高亮耗时: {} {}", cost, TimeUtils.getTimeDes(cost), identity);
        }

        searchResult.setDocuments(result);
        currentProcessingSearchCount.decrementAndGet();
        if(cacheEnabled){
            cache.put(cacheKey, searchResult);
        }
        return searchResult;
    }

    public void highlight(List<Document> documents, String keyWords, List<String> keyWordTerms){
        for(Document document : documents){
            highlight(document, keyWords, keyWordTerms);
        }
    }

    public void highlight(Document document, String keyWords, List<String> keyWordTerms){
        String value = document.getValue();
        if(value.contains(keyWords)){
            document.setValue(value.replace(keyWords, PRE + keyWords + SUF));
            return;
        }
        Collections.sort(keyWordTerms, (a,b)->b.length()-a.length());
        String last = null;
        boolean highlight = false;
        for(String keyWordTerm : keyWordTerms){
            if(last != null && last.contains(keyWordTerm)){
                continue;
            }
            int index = value.indexOf(keyWordTerm);
            if(index > -1) {
                highlight = true;
                value = value.replace(keyWordTerm, PRE + keyWordTerm + SUF);
                if(last == null){
                    last = keyWordTerm;
                }
            }
        }
        if(highlight) {
            document.setValue(value);
        }
    }

    public void createIndex(Document document){
        long start = System.currentTimeMillis();
        indexSingle(document);
        indexTotalCost.addAndGet(System.currentTimeMillis()-start);
    }

    public void deleteIndex(int documentId){
        if(deleteOldIndexIfExist(documentId)){
            LOGGER.debug("文档索引删除成功, documentId: {}", documentId);
        }else{
            LOGGER.warn("要删除的文档索引不存在, documentId: {}", documentId);
        }
    }

    public void updateIndex(Document document){
        long start = System.currentTimeMillis();
        indexSingle(document);
        indexTotalCost.addAndGet(System.currentTimeMillis()-start);
    }

    private void indexSingle(Document document){
        deleteOldIndexIfExist(document.getId());
        int indexId = indexIdGenerator.incrementAndGet();
        List<String> terms = tokenize(document.getValue());
        document.addTerms(terms);
        for(String term : terms){
            INVERTED_INDEX.putIfAbsent(term, Collections.newSetFromMap(new ConcurrentHashMap<>()));
            INVERTED_INDEX.get(term).add(indexId);
        }
        INDEX_TO_DOCUMENT.put(indexId, document.getId());
        DOCUMENT_TO_INDEX.put(document.getId(), indexId);
        DOCUMENT.put(document.getId(), document);
    }

    private boolean deleteOldIndexIfExist(int documentId){
        Integer indexId = DOCUMENT_TO_INDEX.get(documentId);
        if(indexId != null){
            INDEX_TO_DOCUMENT.remove(indexId);
            DOCUMENT_TO_INDEX.remove(documentId);
            DOCUMENT.remove(documentId);
            LOGGER.debug("删除文档索引, documentId: {}", documentId);
            return true;
        }
        return false;
    }

    public void index(Map<Integer, Document> documents){
        long start = System.currentTimeMillis();
        documents.values().parallelStream().forEach(document -> {
            try {
                indexSingle(document);
            }catch (Exception e){
                LOGGER.error("索引数据失败", e);
            }
        });
        documents.clear();
        indexTotalCost.addAndGet(System.currentTimeMillis()-start);
        LOGGER.info(getIndexStatus());
    }

    public List<String> tokenize(String text){
        return tokenize(text, true);
    }

    public boolean isEnglish(String text){
        boolean english = true;
        for(char c : text.toCharArray()){
            if(!isEnglish(c)){
                english = false;
            }
        }
        return english;
    }

    public boolean isEnglish(char c){
        //大部分字符在这个范围
        if(c > 'z' && c < 'Ａ'){
            return false;
        }
        if(c < 'A'){
            return false;
        }
        if(c > 'Z' && c < 'a'){
            return false;
        }
        if(c > 'Ｚ' && c < 'ａ'){
            return false;
        }
        if(c > 'ｚ'){
            return false;
        }
        return true;
    }

    public boolean notContainChinese(String text){
        for(char c : text.toCharArray()){
            if(isChinese(c)){
                return false;
            }
        }
        return true;
    }

    public boolean isChinese(String text){
        boolean chinese = true;
        for(char c : text.toCharArray()){
            if(!isChinese(c)){
                chinese = false;
            }
        }
        return chinese;
    }

    public boolean isChinese(char c){
        return c >= '\u4e00' && c <= '\u9fa5';
    }

    public String extractChinese(String text){
        StringBuilder normal = new StringBuilder();
        for(char c : text.toCharArray()){
            if(isChinese(c)){
                normal.append(c);
            }
        }
        return normal.toString().toLowerCase();
    }

    public List<String> tokenize(String text, boolean generatePinyin){
        if(StringUtils.isBlank(text)){
            return Collections.EMPTY_LIST;
        }
        List<String> tokens = new ArrayList<>();
        // n gram
        int nGramMax = text.length();
        if(nGramMax > maxNgram){
            nGramMax = maxNgram;
        }
        for(int i=1; i<=nGramMax; i++){
            for(int j=0; j<text.length()-i+1; j++){
                if(text.length() - j >= i){
                    String token = text.substring(j, j+i);
                    addWithPinyin(tokens, token, generatePinyin);
                }
            }
        }
        return tokens;
    }

    private void addWithPinyin(List<String> tokens, String token, boolean generatePinyin){
        token = token.toLowerCase();
        if(!tokens.contains(token)) {
            tokens.add(token);
        }
        if(generatePinyin) {
            // 支持 全拼 和 首字母拼音 混合搜索
            // 如: haoxs
            if(token.length() == 1 && isChinese(token)){
                addWithPinyin(tokens, getAllPinYin(token.charAt(0)).stream().filter(pinYin->pinYin.length()>1).collect(Collectors.toList()));
            }else if(token.length() == 2 && isChinese(token)){
                for(String first : getAllPinYin(token.charAt(0))){
                    for(String second : getAllPinYin(token.charAt(1))){
                        tokens.add(first+second);
                    }
                }
            }else if(token.length() == 3 && isChinese(token)){
                for(String first : getAllPinYin(token.charAt(0))){
                    for(String second : getAllPinYin(token.charAt(1))){
                        for(String third : getAllPinYin(token.charAt(2))){
                            tokens.add(first+second+third);
                        }
                    }
                }
            }else {
                addWithPinyin(tokens, getAcronymPinYin(token));
            }
        }
    }

    private void addWithPinyin(List<String> tokens, List<String> pinYins){
        if (pinYins != null && !pinYins.isEmpty()) {
            pinYins.forEach(pinYin->addWithPinyin(tokens, pinYin));
        }
    }

    private void addWithPinyin(List<String> tokens, String pinYin){
        if (pinYin != null) {
            if (!tokens.contains(pinYin)) {
                tokens.add(pinYin);
            }
        }
    }

    public String getAcronymPinYin(String words){
        return getPinYin(words, true);
    }

    public String getFullPinYin(String words) {
        return getPinYin(words, false);
    }

    public List<String> getAllPinYin(char c){
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        format.setVCharType(HanyuPinyinVCharType.WITH_U_UNICODE);
        Set<String> set = new HashSet<>();
        try {
            String[] pinYinStringArray = PinyinHelper.toHanyuPinyinStringArray(c, format);
            for(String pinYin : pinYinStringArray){
                pinYin = pinYin.toLowerCase().replace("ü", "v");
                if(StringUtils.isBlank(pinYin)){
                    continue;
                }
                set.add(pinYin);
                set.add(String.valueOf(pinYin.charAt(0)));
                charPinYin.add(pinYin);
                if(pinYin.length() > charMaxPinYinLength){
                    charMaxPinYinLength = pinYin.length();
                }
            }
        }catch (Exception e){
            LOGGER.error("获取拼音失败", e);
        }
        return set.stream().sorted().collect(Collectors.toList());
    }

    public String getPinYin(String words, boolean acronym) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        format.setVCharType(HanyuPinyinVCharType.WITH_U_UNICODE);
        char[] chars = words.trim().toCharArray();
        StringBuilder result = new StringBuilder();
        int ignoredCount = 0;
        try {
            for (char c : chars) {
                if(!isChinese(c)){
                    ignoredCount++;
                    result.append(c);
                    continue;
                }
                String[] pinyinStringArray = PinyinHelper.toHanyuPinyinStringArray(c, format);
                if(acronym){
                    result.append(pinyinStringArray[0].charAt(0));
                }else {
                    String pinYin = pinyinStringArray[0].toLowerCase().replace("ü", "v");
                    if(StringUtils.isBlank(pinYin)){
                        continue;
                    }
                    result.append(pinYin);
                    charPinYin.add(pinYin);
                    if(pinYin.length() > charMaxPinYinLength){
                        charMaxPinYinLength = pinYin.length();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("获取拼音失败: "+words, e);
        }
        if(ignoredCount == chars.length){
            return null;
        }
        return result.toString().toLowerCase();
    }

    public List<String> ngramAllPinYin(List<List<String>> pinYins){
        List<String> terms = new ArrayList<>();
        for(List<String> pinYin : pinYins){
            List<String> ngram = ngramPinYin(pinYin);
            if(!ngram.isEmpty()){
                terms.addAll(ngram);
            }
        }
        return terms;
    }
    public List<String> ngramPinYin(List<String> pinYin){
        if(pinYin.isEmpty()){
            return Collections.EMPTY_LIST;
        }
        for(String item : pinYin){
            if(item.length() == 1){
                return Collections.EMPTY_LIST;
            }
        }
        List<String> terms = new ArrayList<>();
        // n gram
        int nGramMax = pinYin.size();
        if(nGramMax > maxNgram){
            nGramMax = maxNgram;
        }
        for (int i = 1; i <= nGramMax; i++) {
            for (int j = 0; j < pinYin.size() - i + 1; j++) {
                if (pinYin.size() - j >= i) {
                    StringBuilder term = new StringBuilder();
                    pinYin.subList(j, j + i).forEach(sub -> term.append(sub));
                    terms.add(term.toString());
                }
            }
        }
        return terms;
    }

    public List<List<String>> factorizeAllPinYin(String fullPinYin){
        List<List<String>> allPinYinCombine = new ArrayList<>();
        List<String> factorizedPyList = new ArrayList<>();
        factorizePinYin(factorizedPyList, fullPinYin);
        for(String factorizedPy : factorizedPyList){
            List<String> combine = new ArrayList<>();
            for(String item : factorizedPy.split(",")){
                if(item.length() == 1){
                    combine.clear();
                    break;
                }
                combine.add(item);
            }
            if(!combine.isEmpty()) {
                allPinYinCombine.add(combine);
            }
        }
        int maxLen = 0;
        for(List<String> item : allPinYinCombine){
            int len = item.stream().mapToInt(py->py.length()).sum();
            if(len > maxLen){
                maxLen = len;
            }
        }
        final int finalMaxLen = maxLen;
        List<List<String>> limitedResult = allPinYinCombine.stream().filter(item->{
            int len = item.stream().mapToInt(py->py.length()).sum();
            return len == finalMaxLen;
        }).collect(Collectors.toList());
        LOGGER.info("对拼音解码进行限制, 限制前: {}, 限制后: {}", allPinYinCombine, limitedResult);
        return limitedResult;
    }

    /**
     * @param factorizedPinYinList 对拼音的切分组合，中间用逗号隔开
     * @param fullPinYin           输入的拼音,要求拼音能够完全分解
     */
    public void factorizePinYin(List<String> factorizedPinYinList, String fullPinYin) {
        //找到最后一个分隔符的下一个字母作为首字母
        int initialLetterIdx = fullPinYin.lastIndexOf(',') + 1;
        //每次分解只分解一个字符的拼音，限定拼音的长度
        int maxLen = charMaxPinYinLength < fullPinYin.length() - initialLetterIdx ? charMaxPinYinLength : fullPinYin.length() - initialLetterIdx;
        for (int i = initialLetterIdx; i < initialLetterIdx + maxLen; i++) {
            //从首字母到i这个substring是否可以组成一个拼音
            if (charPinYin.contains(fullPinYin.substring(initialLetterIdx, i + 1))) {
                //代表已经分解完成
                if (i >= fullPinYin.length() - 1) {
                    //记录分解的结果，递归分支结束
                    factorizedPinYinList.add(fullPinYin);
                } else {
                    //在当前位置加入分割符，进行下一次分解
                    String currentFactorizedPinYin = fullPinYin.substring(0, i + 1) + "," + fullPinYin.substring(i + 1);
                    //递归分解
                    factorizePinYin(factorizedPinYinList, currentFactorizedPinYin);
                }
            }
            //如果已经结束，最后的可以不完全分解,返回已经分解好的组合
            else if (i >= fullPinYin.length() - 1 && initialLetterIdx > 0) {
                factorizedPinYinList.add(fullPinYin.substring(0, initialLetterIdx - 1));
            }
        }
    }

    public static void main(String[] args) {
        ShortTextSearcher shortTextSearcher = new ShortTextSearcher(3);
        shortTextSearcher.index(ShortTextResource.loadShortText());

        AtomicInteger i = new AtomicInteger();
        shortTextSearcher.search("阿里", 500).getDocuments().forEach(doc -> System.out.println(i.incrementAndGet()+". "+doc.getValue()+" "+" ("+doc.getScore()+")"));

        System.out.println(shortTextSearcher.getFullPinYin("儿女"));
    }
}
