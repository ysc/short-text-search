package org.apdplat.search.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ysc on 1/8/17.
 */
public class SearchResult {
    private List<Document> documents = new ArrayList<>();
    private String identity = "unknown";
    private boolean overload;

    public boolean isOverload() {
        return overload;
    }

    public void setOverload(boolean overload) {
        this.overload = overload;
    }

    public List<Document> getDocuments() {
        return documents;
    }

    public void setDocuments(List<Document> documents) {
        this.documents = documents;
    }

    public String getIdentity() {
        return identity;
    }

    public void setIdentity(String identity) {
        this.identity = identity;
    }
}
