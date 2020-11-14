<%--
  Created by IntelliJ IDEA.
  User: ysc
  Date: 1/8/17
  Time: 3:03 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%
    boolean detail = "true".equals(request.getParameter("detail"));
    boolean highlight = !"false".equals(request.getParameter("highlight"));
    String keyWords = request.getParameter("keyWords") == null ? "深圳万科" : request.getParameter("keyWords");
    if("true".equals(request.getParameter("refresh"))){
        String key = "ShortTextSearcher";
        application.setAttribute(key, null);
    }
    int maxNgram = 6;
    try{
        maxNgram = Integer.parseInt(request.getParameter("maxNgram"));
    }catch (Exception e){
        //
    }
    int topN = 10;
    try{
        topN = Integer.parseInt(request.getParameter("topN"));
    }catch (Exception e){
        //
    }
%>
<html>
<head>
    <title>短文本搜索</title>
    <!-- jquery -->
    <script type="text/javascript" src="jquery/jquery-2.2.1.min.js"></script>
    <script type="text/javascript" src="js/utils.js"></script>
    <script type="text/javascript">
        var contextPath = '<%=request.getContextPath()%>';
        var searching = false;
        var lastUrl = "";
        function search() {
            if(searching){
                return;
            }
            searching = true;
            var keyWords = document.getElementById("keyWords").value;
            if(keyWords.length > 30){
                keyWords = keyWords.substr(0, 30);
                document.getElementById("keyWords").value = keyWords;
            }
            var topN = document.getElementById("topN").value;
            var url = contextPath+"/search.jsp?status=true&type=html&topN="+topN+"&keyWords="+encodeURIComponent(keyWords)+"&maxNgram=<%=maxNgram%>"+"&highlight=<%=highlight%>"+"&detail=<%=detail%>";
            if(lastUrl == url){
                searching = false;
                return;
            }
            document.getElementById("search-result").innerHTML = "正在搜索......";
            lastUrl = url;
            $.get(url, function (data, status) {
                document.getElementById("search-result").innerHTML = data;
                searching = false;
            }).fail(function(e) {
                Utils.log("获取数据出错: "+JSON.stringify(e));
                searching = false;
            });
        }
        function refresh(){
            var keyWords = document.getElementById("keyWords").value;
            var topN = document.getElementById("topN").value;
            var url = contextPath+"/index.jsp?topN="+topN+"&keyWords="+keyWords+"&highlight=<%=highlight%>"+"&detail=<%=detail%>";
            location.href = url;
        }
    </script>
</head>
<body onload="search();">
<p>
    展示结果条数: <input id="topN" value="<%=topN%>" onkeyup="search();"><br/>
    输入搜索词条: <input id="keyWords" value="<%=keyWords%>" size="80" onkeyup="search();">
</p>
<!--
<h3>
    <a href="#"><span onclick="search();">搜索</span></a>
</h3>
-->

<div id="search-result">
</div>

<h3><a target="_blank" href="cache-detail.jsp">查看短文本搜索缓存命中情况</a></h3>
<h3><a target="_blank" href="search-histories.jsp">查看短文本搜索历史记录</a></h3>
<h3><a target="_blank" href="search-histories.jsp?clear=true">清除短文本搜索历史记录</a></h3>
<br/><br/><br/><br/>
</body>
</html>
