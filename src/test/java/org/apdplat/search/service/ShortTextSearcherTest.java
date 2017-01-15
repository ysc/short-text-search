package org.apdplat.search.service;

import org.apdplat.search.model.Document;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by ysc on 1/8/17.
 */
public class ShortTextSearcherTest {
    @Test
    public void tokenize() throws Exception {
        ShortTextSearcher shortTextSearcher = new ShortTextSearcher(6);
        String actualValue = shortTextSearcher.tokenize(null, false).toString();
        String expectedValue = "[]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("", false).toString();
        expectedValue = "[]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("人", false).toString();
        expectedValue = "[人]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("人名", false).toString();
        expectedValue = "[人, 名, 人名]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("杨尚川", false).toString();
        expectedValue = "[杨, 尚, 川, 杨尚, 尚川, 杨尚川]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("中华人民", false).toString();
        expectedValue = "[中, 华, 人, 民, 中华, 华人, 人民, 中华人, 华人民, 中华人民]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("中华人民好", false).toString();
        expectedValue = "[中, 华, 人, 民, 好, 中华, 华人, 人民, 民好, 中华人, 华人民, 人民好, 中华人民, 华人民好, 中华人民好]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("中华人民好玩", false).toString();
        expectedValue = "[中, 华, 人, 民, 好, 玩, 中华, 华人, 人民, 民好, 好玩, 中华人, 华人民, 人民好, 民好玩, 中华人民, 华人民好, 人民好玩, 中华人民好, 华人民好玩, 中华人民好玩]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("中华人民共和国", false).toString();
        expectedValue = "[中, 华, 人, 民, 共, 和, 国, 中华, 华人, 人民, 民共, 共和, 和国, 中华人, 华人民, 人民共, 民共和, 共和国, 中华人民, 华人民共, 人民共和, 民共和国, 中华人民共, 华人民共和, 人民共和国, 中华人民共和, 华人民共和国]";
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void tokenizeWithPinyin() throws Exception {
        ShortTextSearcher shortTextSearcher = new ShortTextSearcher(6);
        String actualValue = shortTextSearcher.tokenize(null).toString();
        String expectedValue = "[]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("").toString();
        expectedValue = "[]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("人").toString();
        expectedValue = "[人, ren]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("人名").toString();
        expectedValue = "[人, ren, 名, ming, 人名, rm, rming, renm, renming]";
        assertEquals(expectedValue, actualValue);

        actualValue = shortTextSearcher.tokenize("杨尚川").toString();
        expectedValue = "[杨, yang, 尚, shang, 川, chuan, 杨尚, ys, yshang, yangs, yangshang, 尚川, sc, schuan, shangc, shangchuan, 杨尚川, ysc, yschuan, yshangc, yshangchuan, yangsc, yangschuan, yangshangc, yangshangchuan]";
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void search() throws Exception {
        ShortTextSearcher shortTextSearcher = new ShortTextSearcher(3);
        shortTextSearcher.index(ShortTextResource.loadShortText());
        List<Document> actualValue = shortTextSearcher.search("深圳万科", 1).getDocuments();
        assertEquals(1, actualValue.size());
        long expectedId = 65317;
        assertEquals(expectedId, actualValue.get(0).getId());
        String expectedName = "<font color='red'>深圳万科</font>";
        assertEquals(expectedName, actualValue.get(0).getValue());

        actualValue = shortTextSearcher.search("深圳万科", 1, false).getDocuments();
        expectedName = "深圳万科";
        assertEquals(expectedName, actualValue.get(0).getValue());
    }

    @Test
    public void search2() throws Exception {
        // 不启用搜索结果缓存功能
        ShortTextSearcher shortTextSearcher = new ShortTextSearcher(6, false);

        List<Document> actualValue = shortTextSearcher.search("杨尚川历险记", 1, false).getDocuments();
        assertEquals(0, actualValue.size());

        Document document = new Document();
        document.setId(-100);
        document.setValue("杨尚川历险记");

        shortTextSearcher.createIndex(document);
        shortTextSearcher.saveIndex();

        actualValue = shortTextSearcher.search("杨尚川历险记", 1, false).getDocuments();
        assertEquals(1, actualValue.size());
        long expectedId = -100;
        assertEquals(expectedId, actualValue.get(0).getId());
        String expectedName = "杨尚川历险记";
        assertEquals(expectedName, actualValue.get(0).getValue());

        shortTextSearcher.deleteIndex(document.getId());

        actualValue = shortTextSearcher.search("杨尚川历险记", 1, false).getDocuments();
        assertEquals(0, actualValue.size());
    }
}