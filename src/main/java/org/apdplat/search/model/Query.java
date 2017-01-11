package org.apdplat.search.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ysc on 1/8/17.
 */
public class Query {
    private List<String> keyWordTerms = new ArrayList<>();
    private boolean hasNgramPinYin;

    public boolean isEmpty(){
        return keyWordTerms.isEmpty();
    }

    public boolean containsKeyWordTerm(String keyWordTerm){
        return keyWordTerms.contains(keyWordTerm);
    }

    public List<String> getKeyWordTerms() {
        return keyWordTerms;
    }

    public void addKeyWordTerm(String keyWordTerm) {
        if(!containsKeyWordTerm(keyWordTerm)) {
            this.keyWordTerms.add(keyWordTerm);
        }
    }

    public void addKeyWordTerms(List<String> keyWordTerms) {
        for(String keyWordTerm : keyWordTerms){
            addKeyWordTerm(keyWordTerm);
        }
    }

    public boolean hasNgramPinYin() {
        return hasNgramPinYin;
    }

    public void hasNgramPinYin(boolean hasNgramPinYin) {
        this.hasNgramPinYin = hasNgramPinYin;
    }
}
