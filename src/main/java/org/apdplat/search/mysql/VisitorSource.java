package org.apdplat.search.mysql;

import org.apache.commons.lang3.StringUtils;
import org.apdplat.search.model.Document;
import org.apdplat.search.service.SearchService;
import org.apdplat.search.utils.MySQLUtils;
import org.apdplat.search.utils.RecognitionTool;
import org.apdplat.search.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;

/**
 * Created by ysc on 22/04/2020.
 */
public class VisitorSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(VisitorSource.class);

    public static int index(LocalDateTime s, LocalDateTime end, String type){
        int count=0;
        long start = System.currentTimeMillis();
        LOGGER.info("开始导出im_visitor_wechat");
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return 0;
        }
        String format = "yyyy-MM-dd"+" 00:00:00";
        if("minute".equals(type)){
            format = "yyyy-MM-dd"+" HH:mm:00";
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            con.setAutoCommit(false);
            while(s.isBefore(end)){
                String b1 = TimeUtils.toString(s, format);
                String b2 = TimeUtils.toString("minute".equals(type) ? s.plusMinutes(1) : s.plusDays(1), format);

                String sql = "select unionid, phone, nickname, created_at  from im_visitor_wechat WHERE created_at BETWEEN '"+b1+"' AND '"+b2+"';";
                //LOGGER.info("sql: {}", sql);
                pst = con.prepareStatement(sql);
                rs = pst.executeQuery();
                while (rs.next()) {
                    if(b2.equals(TimeUtils.toString(rs.getTimestamp("created_at").getTime(), "yyyy-MM-dd HH:mm:ss"))){
                        LOGGER.info("忽略超出边界的数据: {}", b2);
                        continue;
                    }
                    String unionid = process(rs.getString("unionid"));
                    if(StringUtils.isBlank(unionid)){
                        LOGGER.error("发现空的unionid");
                        continue;
                    }
                    String phone = process(rs.getString("phone"));
                    String nickname = process(rs.getString("nickname"));
                    StringBuilder str = new StringBuilder();
                    for(char c : phone.toCharArray()){
                        if(RecognitionTool.isNumber(c)){
                            str.append(c);
                            continue;
                        }
                    }
                    if(str.length() > 0){
                        str.append(",");
                    }
                    for(char c : nickname.toCharArray()){
                        if(RecognitionTool.isEnglish(c)){
                            str.append(c);
                            continue;
                        }
                        if(RecognitionTool.isNumber(c)){
                            str.append(c);
                            continue;
                        }
                        if(RecognitionTool.isChineseCharAndLengthAtLeastOne(new Character(c).toString())){
                            str.append(c);
                            continue;
                        }
                    }
                    String text = str.toString();
                    if(StringUtils.isNotBlank(text)) {
                        SearchService.getShortTextSearcher().updateIndex(new Document(unionid, text));
                    }

                    count++;
                    if (count % 10000 == 0) {
                        LOGGER.info("导出条数: {}", count);
                    }
                }
                rs.close();
                s = s.plusDays(1);
                //LOGGER.info("count: {}", c);
            }
            LOGGER.info("导出im_visitor_wechat成功, 耗时: {}, 导出条数: {}, type: {}, start: {}, end: {}",
                    TimeUtils.getTimeDes(System.currentTimeMillis()-start), count, type, TimeUtils.toString(s), TimeUtils.toString(end));
        }catch (Exception e) {
            LOGGER.error("导出im_visitor_wechat失败", e);
        } finally {
            MySQLUtils.close(con, pst);
        }
        return count;
    }
    public static String process(String value){
        if("NULL".equalsIgnoreCase(value)){
            return "";
        }
        return StringUtils.isBlank(value) ? "" : value;
    }

    public static String get(Connection con, String unionid){
        if(con == null || StringUtils.isBlank(unionid)){
            return "";
        }
        String sql = "select nickname, phone from im_visitor_wechat where unionid='"+unionid+"'  ORDER BY created_at DESC LIMIT 1";
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = con.prepareStatement(sql);
            rs = pst.executeQuery();
            if (rs.next()) {
                String nickname = process(rs.getString("nickname"));
                String phone = process(rs.getString("phone"));
                return process(phone)+"  "+process(nickname);
            }
            rs.close();
        }catch (Exception e) {
            LOGGER.error("查询访客信息异常", e);
        }
        return "";
    }
}
