<%--
  Created by IntelliJ IDEA.
  User: ysc
  Date: 1/8/17
  Time: 3:03 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page import="org.apdplat.search.model.Document" %>
<%@ page import="org.apdplat.search.service.ShortTextSearcher" %>
<%@ page import="org.apdplat.search.utils.TimeUtils" %>
<%@ page import="org.apdplat.search.service.SearchService" %>
<%@ page import="java.util.List" %>
<%@ page import="org.apdplat.search.model.SearchResult" %>
<%@ page import="java.net.URLEncoder" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    boolean status = "true".equals(request.getParameter("status"));
    ShortTextSearcher shortTextSearcher = SearchService.getShortTextSearcher();
    if(shortTextSearcher == null){
        out.println("search service unavailable");
        return;
    }
    boolean highlight = "true".equals(request.getParameter("highlight"));
    String keyWords = request.getParameter("keyWords") == null ? "深圳万科" : request.getParameter("keyWords");
    String id = request.getParameter("id");
    int topN = 100;
    try{
        topN = Integer.parseInt(request.getParameter("topN"));
    }catch (Exception e){
        //
    }
    if("html".equals(request.getParameter("type"))) {
        long start = System.currentTimeMillis();
        SearchResult searchResult = shortTextSearcher.search(keyWords, topN, highlight);
        List<Document> documents = searchResult.getDocuments();
        long cost = System.currentTimeMillis() - start;
        shortTextSearcher.getLogger().info("{} 搜索接口总耗时: {} {}", cost, TimeUtils.getTimeDes(cost), searchResult.getIdentity());
        StringBuilder html = new StringBuilder();
        html.append("搜索耗时: ")
                .append(TimeUtils.getTimeDes(cost))
                .append("<br/>")
                .append("结果条数: ")
                .append(documents.size())
                .append("<br/>");
        html.append("<table border=\"1\">")
                .append("<tr><th>序号</th><th>ID</th><th>短文本</th><th>搜索评分</th></tr>\n");
        int i=1;
        for (Document document : documents) {
            html.append("<tr>")
                    .append("<td>")
                    .append(i++)
                    .append("</td>")
                    .append("<td>")
                    .append(document.getId())
                    .append("</td>")
                    .append("<td>")
                    .append(document.getValue())
                    .append("</td>")
                    .append("<td>")
                    .append("<a target=\"_blank\" href=\"search.jsp?explain=true&id=")
                    .append(document.getId())
                    .append("&keyWords=")
                    .append(URLEncoder.encode(keyWords, "utf-8"))
                    .append("\">")
                    .append(document.getScore())
                    .append("</a>")
                    .append("</td>")
                    .append("</tr>");
        }
        html.append("</table>");
        if(status) {
            String indexStatus = shortTextSearcher.getIndexStatus();
            if (indexStatus != null) {
                html.append("<br/><font color=\"red\">索引状态</font><br/>").append(indexStatus.replace("\n", "<br/>").replace("\t", "&nbsp&nbsp&nbsp&nbsp"));
            }
            String searchStatus = shortTextSearcher.getSearchStatus();
            if (searchStatus != null) {
                html.append("<br/><font color=\"red\">搜索状态</font><br/>").append(searchStatus.replace("\n", "<br/>").replace("\t", "&nbsp&nbsp&nbsp&nbsp"));
            }
            String cacheStatus = shortTextSearcher.getCacheStatus();
            if (cacheStatus != null) {
                html.append("<br/><font color=\"red\">缓存状态</font><br/>").append(cacheStatus.replace("\n", "<br/>").replace("\t", "&nbsp&nbsp&nbsp&nbsp"));
            }
        }
        out.println(html);
    }else{
        response.setContentType("application/json;charset=UTF-8");
        response.setHeader("Access-Control-Allow-Origin", "*");
        long start = System.currentTimeMillis();
        SearchResult searchResult = shortTextSearcher.search(keyWords, topN, highlight);
        List<Document> documents = searchResult.getDocuments();
        long cost = System.currentTimeMillis() - start;
        shortTextSearcher.getLogger().info("{} 搜索接口总耗时: {} {}", cost, TimeUtils.getTimeDes(cost), searchResult.getIdentity());
        StringBuilder json = new StringBuilder();
        json.append("{\n")
                .append("\"cost\":\"")
                .append(TimeUtils.getTimeDes(cost))
                .append("\",\n")
                .append("\"maxNgram\":")
                .append(shortTextSearcher.getMaxNgram())
                .append(",\n")
                .append("\"size\":")
                .append(documents.size());

        if(searchResult.isOverload()){
            json.append(",\n")
                    .append("\"message\":\"")
                    .append("search service overload")
                    .append("\",\n");
        }

        if(status) {
            String indexStatus = shortTextSearcher.getIndexStatus();
            if (indexStatus != null) {
                json.append(",\n\"indexStatus\":\n\"");
                json.append(indexStatus.replace("\n", "; "))
                    .append("\"");
            }
            String searchStatus = shortTextSearcher.getSearchStatus();
            if (searchStatus != null) {
                json.append(",\n\"searchStatus\":\n\"");
                json.append(searchStatus.replace("\n", "; "));
            }
        }
        json.append("\",\n\"result\":\n[");
        for (Document document : documents) {
            json.append("{")
                    .append("\"id\":")
                    .append(document.getId())
                    .append(",")
                    .append("\"value\":\"")
                    .append(document.getValue())
                    .append("\",")
                    .append("\"score\":")
                    .append(document.getScore())
                    .append("},\n");
        }
        if (documents.size() > 0) {
            json.setLength(json.length() - 2);
        }
        json.append("]\n}\n");
        out.println(json);
    }
%>