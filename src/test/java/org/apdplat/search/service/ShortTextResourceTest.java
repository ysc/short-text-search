package org.apdplat.search.service;

import org.apdplat.search.model.Document;
import org.junit.Test;

import java.util.Map;

import static junit.framework.TestCase.assertEquals;

/**
 * Created by ysc on 1/8/17.
 */
public class ShortTextResourceTest {

    @Test
    public void testLoadShortText() throws Exception {
        Map<Integer, Document> shortText = ShortTextResource.loadShortText();
        int actualValue = shortText.size();
        int expectedValue = 4;
        assertEquals(expectedValue, actualValue);
    }
}