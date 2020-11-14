package org.apdplat.search.model;

import java.net.URLEncoder;
import java.util.Objects;

/**
 * Created by ysc on 1/8/17.
 */
public class Document implements Comparable{
    private String id;
    private String value="";
    private int score;


    public Document(){}

    public Document(String id, String value) {
        this.id = id;
        this.value = value;
    }

    public Document clone(){
        Document document = new Document();
        document.id = id;
        document.value = value;
        document.score = 0;
        return document;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Document document = (Document) o;

        return id == document.id;

    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Document{" +
                "id=" + id +
                ", value='" + value + '\'' +
                ", score=" + score +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        return id.compareTo(((Document)o).getId());
    }

    public static void main(String[] args) {
        System.out.println(URLEncoder.encode("晓宇12345ysc"));
    }
}