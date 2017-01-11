package org.apdplat.search.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Created by ysc on 1/8/17.
 */
public class TimeUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeUtils.class);

    private TimeUtils(){}

    public static long fromString(String time) {
        return fromString(time, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 将时间裁剪到分钟
     * @param time
     * @return
     */
    public static long trimToMinute(long time){
        return fromString(toString(time, "yyyy-MM-dd HH:mm"), "yyyy-MM-dd HH:mm");
    }

    public static long fromString(String time, String pattern) {
        if(time == null || "null".equals(time)){
            return -1;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date = null;
        try {
            date = simpleDateFormat.parse(time);
            if (date != null) {
                return date.getTime();
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return -1;
    }

    public static String getTimeDes(Long ms) {
        //处理参数为NULL的情况
        if(ms == null || ms == 0){
            return "0毫秒";
        }
        boolean minus = false;
        if(ms < 0){
            minus = true;
            ms = -ms;
        }
        int ss = 1000;
        int mi = ss * 60;
        int hh = mi * 60;
        int dd = hh * 24;

        long day = ms / dd;
        long hour = (ms - day * dd) / hh;
        long minute = (ms - day * dd - hour * hh) / mi;
        long second = (ms - day * dd - hour * hh - minute * mi) / ss;
        long milliSecond = ms - day * dd - hour * hh - minute * mi - second * ss;

        StringBuilder str=new StringBuilder();
        if(day>0){
            str.append(day).append("天,");
        }
        if(hour>0){
            str.append(hour).append("小时,");
        }
        if(minute>0){
            str.append(minute).append("分钟,");
        }
        if(second>0){
            str.append(second).append("秒,");
        }
        if(milliSecond>0){
            str.append(milliSecond).append("毫秒,");
        }
        if(str.length()>0){
            str.setLength(str.length() - 1);
        }

        if(minus){
            return "-"+str.toString();
        }

        return str.toString();
    }

    /**
     * 移除毫秒, 对齐到秒
     * @param time 2015-10-23 10:35:22
     * @return 移除22变为2015-10-23 10:35
     */
    public static LocalDateTime parseIgnoreMilli(String time){
        return LocalDateTime.ofInstant(new Date(fromString(time, "yyyy-MM-dd HH:mm")).toInstant(), ZoneId.systemDefault());
    }

    public static LocalDateTime parse(String text){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.parse(text, formatter);
    }

    public static String toString(LocalDateTime time){
        return toString(time, "yyyy-MM-dd HH:mm:ss");
    }

    public static String toString(LocalDateTime time, String pattern){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return time.format(formatter);
    }

    public static String toString(long time){
        return toString(time, "yyyy-MM-dd HH:mm:ss");
    }

    public static String toString(long time, String pattern){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.format(new Date(time));
    }
}
