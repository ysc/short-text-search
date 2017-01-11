<%@ page import="org.apdplat.search.model.Document" %>
<%@ page import="org.apdplat.search.service.ShortTextSearcher" %>
<%@ page import="org.apdplat.search.service.SearchService" %>
<%@ page import="java.util.List" %>
<%@ page import="org.apdplat.search.utils.TimeUtils" %>
<%@ page import="org.apdplat.search.model.SearchResult" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    response.setContentType("application/json;charset=UTF-8");
    response.setHeader("Access-Control-Allow-Origin", "*");

    ShortTextSearcher shortTextSearcher = SearchService.getShortTextSearcher();
    if(shortTextSearcher == null){
        StringBuilder json = new StringBuilder();
        json.append("[]");
        out.println(json.toString());
        return;
    }
    boolean highlight = "true".equals(request.getParameter("highlight"));

    String keyWords = request.getParameter("kw") == null ? "万科" : request.getParameter("kw");
    int topN = 5;
    try{
        topN = Integer.parseInt(request.getParameter("topN"));
    }catch (Exception e){
        //
    }

    long start = System.currentTimeMillis();
    SearchResult searchResult = shortTextSearcher.search(keyWords, topN, highlight);
    List<Document> documents = searchResult.getDocuments();
    long cost = System.currentTimeMillis() - start;
    shortTextSearcher.getLogger().info("{} 搜索接口总耗时: {} {}", cost, TimeUtils.getTimeDes(cost), searchResult.getIdentity());

    StringBuilder json = new StringBuilder();
    json.append("[\n");

    int i = 1;
    for (Document document : documents) {
        json.append("{")
                .append("\"id\":")
                .append(document.getId())
                .append(",")
                .append("\"value\":\"")
                .append(document.getValue())
                .append("\"}");
        if(i++ < documents.size()){
            json.append(",\n");
        }
    }

    json.append("\n]");

    out.println(json);
%>