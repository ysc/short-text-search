package org.apdplat.search.service;

import org.apache.commons.lang3.StringUtils;
import org.apdplat.search.model.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ysc on 1/8/17.
 */
public class ShortTextResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShortTextResource.class);

    private static final Map<Integer, Document> DOCUMENT_MAP = new HashMap<>();

    static {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(ShortTextResource.class.getResourceAsStream("/short_text.txt")))) {
            int id = 1;
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    line = line.trim();
                    if(StringUtils.isBlank(line) || line.startsWith("#")){
                        continue;
                    }
                    Document document = new Document();
                    document.setId(id);
                    document.setValue(line);
                    DOCUMENT_MAP.put(id, document);
                    id++;
                }catch (Throwable e){
                    LOGGER.error("错误的数据: "+line, e);
                }
            }
        }catch (Throwable e){
            LOGGER.error("解析数据出错", e);
        }
    }

    public static Map<Integer, Document> loadShortText(){
        return DOCUMENT_MAP;
    }

    public static void main(String[] args) {
        System.out.println("short text count: "+DOCUMENT_MAP.size());
    }
}
