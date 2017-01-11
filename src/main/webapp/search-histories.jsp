<%--
  Created by IntelliJ IDEA.
  User: ysc
  Date: 1/8/17
  Time: 3:03 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page import="org.apdplat.search.service.ShortTextSearcher" %>
<%@ page import="org.apache.commons.lang3.StringUtils" %>
<%@ page import="org.apdplat.search.service.SearchService" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    ShortTextSearcher shortTextSearcher = SearchService.getShortTextSearcher();
    if(shortTextSearcher == null){
        out.println("search service unavailable");
        return;
    }
    if("true".equals(request.getParameter("clear"))){
        shortTextSearcher.clearSearchHistories();
        out.println("搜索历史记录已经被清除");
        return;
    }
%>
<html>
<head>
    <title>短文本搜索历史记录</title>
    <!-- jquery -->
    <script type="text/javascript" src="jquery/jquery-2.2.1.min.js"></script>
    <script type="text/javascript" src="js/utils.js"></script>
    <script type="text/javascript">
        var contextPath = '<%=request.getContextPath()%>';
    </script>
</head>
<body>
<%
    String histories = shortTextSearcher.getSearchHistories();
    if(StringUtils.isNotBlank(histories)){
        String[] lines = histories.split("\\n");
%>
    <p>
        <h3>短文本搜索历史记录(<%=lines.length%>)</h3>
        <table border="1">
            <tr><th>序号</th><th>搜索关键词</th><th>搜索次数</th></tr>
        <%
            for(String line : lines){
                String[] attr = line.split("\\t");
        %>
            <tr><td><%=attr[0]%></td><td><a target="_blank" href="index.jsp?keyWords=<%=attr[1]%>"><%=attr[1]%></a></td><td><%=attr[2]%></td></tr>
        <%
            }
        %>
        </table>
    </p>
<%
    }else{
        out.print("还没有搜索历史记录");
    }
%>
</body>
</html>
