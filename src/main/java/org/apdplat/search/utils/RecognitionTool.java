package org.apdplat.search.utils;

/**
 * Created by ysc on 22/04/2020.
 */

import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * 分词特殊情况识别工具
 * 如英文单词、数字、时间等
 * @author 杨尚川
 */
public class RecognitionTool {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RecognitionTool.class);
    private static final Pattern PATTERN_ONE = Pattern.compile("^[\\u4e00-\\u9fa5]+$");
    //'〇'不常用，放到最后
    private static final char[] chineseNumbers = {'一','二','三','四','五','六','七','八','九','十','百','千','万','亿','零','壹','贰','叁','肆','伍','陆','柒','捌','玖','拾','佰','仟','〇'};
    /**
     * 至少出现一次中文字符，且以中文字符开头和结束
     * @param word
     * @return
     */
    public static boolean isChineseCharAndLengthAtLeastOne(String word){
        if(PATTERN_ONE.matcher(word).find()){
            return true;
        }
        return false;
    }
    /**
     * 识别文本（英文单词、数字、时间等）
     * @param text 识别文本
     * @return 是否识别
     */
    public static boolean recog(final String text){
        return recog(text, 0, text.length());
    }
    /**
     * 识别文本（英文单词、数字、时间等）
     * @param text 识别文本
     * @param start 待识别文本开始索引
     * @param len 识别长度
     * @return 是否识别
     */
    public static boolean recog(final String text, final int start, final int len){
        return isEnglishAndNumberMix(text, start, len)
                || isFraction(text, start, len)
                || isChineseNumber(text, start, len);
    }
    /**
     * 小数和分数识别
     * @param text 识别文本
     * @return 是否识别
     */
    public static boolean isFraction(final String text){
        return isFraction(text, 0, text.length());
    }
    /**
     * 小数和分数识别
     * @param text 识别文本
     * @param start 待识别文本开始索引
     * @param len 识别长度
     * @return 是否识别
     */
    public static boolean isFraction(final String text, final int start, final int len){
        if(len < 3){
            return false;
        }
        int index = -1;
        for(int i=start; i<start+len; i++){
            char c = text.charAt(i);
            if(c=='.' || c=='/' || c=='／' || c=='．' || c=='·'){
                index = i;
                break;
            }
        }
        if(index == -1 || index == start || index == start+len-1){
            return false;
        }
        int beforeLen = index-start;
        return isNumber(text, start, beforeLen) && isNumber(text, index+1, len-(beforeLen+1));
    }
    /**
     * 英文字母和数字混合识别，能识别纯数字、纯英文单词以及混合的情况
     * @param text 识别文本
     * @param start 待识别文本开始索引
     * @param len 识别长度
     * @return 是否识别
     */
    public static boolean isEnglishAndNumberMix(final String text, final int start, final int len){
        for(int i=start; i<start+len; i++){
            char c = text.charAt(i);
            if(!(isEnglish(c) || isNumber(c))){
                return false;
            }
        }
        //指定的字符串已经识别为英文字母和数字混合串
        //下面要判断英文字母和数字混合串是否完整
        if(start>0){
            //判断前一个字符，如果为英文字符或数字则识别失败
            char c = text.charAt(start-1);
            if(isEnglish(c) || isNumber(c)){
                return false;
            }
        }
        if(start+len < text.length()){
            //判断后一个字符，如果为英文字符或数字则识别失败
            char c = text.charAt(start+len);
            if(isEnglish(c) || isNumber(c)){
                return false;
            }
        }
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("识别出英文字母和数字混合串：" + text.substring(start, start + len));
        }
        return true;
    }
    /**
     * 英文单词识别
     * @param text 识别文本
     * @return 是否识别
     */
    public static boolean isEnglish(final String text){
        return isEnglish(text, 0, text.length());
    }
    /**
     * 英文单词识别
     * @param text 识别文本
     * @param start 待识别文本开始索引
     * @param len 识别长度
     * @return 是否识别
     */
    public static boolean isEnglish(final String text, final int start, final int len){
        for(int i=start; i<start+len; i++){
            char c = text.charAt(i);
            if(!isEnglish(c)){
                return false;
            }
        }
        //指定的字符串已经识别为英文串
        //下面要判断英文串是否完整
        if(start>0){
            //判断前一个字符，如果为英文字符则识别失败
            char c = text.charAt(start-1);
            if(isEnglish(c)){
                return false;
            }
        }
        if(start+len < text.length()){
            //判断后一个字符，如果为英文字符则识别失败
            char c = text.charAt(start+len);
            if(isEnglish(c)){
                return false;
            }
        }
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("识别出英文单词：" + text.substring(start, start + len));
        }
        return true;
    }
    /**
     * 英文字符识别，包括大小写，包括全角和半角
     * @param c 字符
     * @return 是否是英文字符
     */
    public static boolean isEnglish(char c){
        //大部分字符在这个范围
        if(c > 'z' && c < 'Ａ'){
            return false;
        }
        if(c < 'A'){
            return false;
        }
        if(c > 'Z' && c < 'a'){
            return false;
        }
        if(c > 'Ｚ' && c < 'ａ'){
            return false;
        }
        if(c > 'ｚ'){
            return false;
        }
        return true;
    }
    /**
     * 数字识别
     * @param text 识别文本
     * @return 是否识别
     */
    public static boolean isNumber(final String text){
        return isNumber(text, 0, text.length());
    }
    /**
     * 数字识别
     * @param text 识别文本
     * @param start 待识别文本开始索引
     * @param len 识别长度
     * @return 是否识别
     */
    public static boolean isNumber(final String text, final int start, final int len){
        for(int i=start; i<start+len; i++){
            char c = text.charAt(i);
            if(!isNumber(c)){
                return false;
            }
        }
        //指定的字符串已经识别为数字串
        //下面要判断数字串是否完整
        if(start>0){
            //判断前一个字符，如果为数字字符则识别失败
            char c = text.charAt(start-1);
            if(isNumber(c)){
                return false;
            }
        }
        if(start+len < text.length()){
            //判断后一个字符，如果为数字字符则识别失败
            char c = text.charAt(start+len);
            if(isNumber(c)){
                return false;
            }
        }
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("识别出数字：" + text.substring(start, start + len));
        }
        return true;
    }
    /**
     * 阿拉伯数字识别，包括全角和半角
     * @param c 字符
     * @return 是否是阿拉伯数字
     */
    public static boolean isNumber(char c){
        //大部分字符在这个范围
        if(c > '9' && c < '０'){
            return false;
        }
        if(c < '0'){
            return false;
        }
        if(c > '９'){
            return false;
        }
        return true;
    }
    /**
     * 中文数字识别，包括大小写
     * @param text 识别文本
     * @return 是否识别
     */
    public static boolean isChineseNumber(final String text){
        return isChineseNumber(text, 0, text.length());
    }
    /**
     * 中文数字识别，包括大小写
     * @param text 识别文本
     * @param start 待识别文本开始索引
     * @param len 识别长度
     * @return 是否识别
     */
    public static boolean isChineseNumber(final String text, final int start, final int len){
        for(int i=start; i<start+len; i++){
            char c = text.charAt(i);
            boolean isChineseNumber = false;
            for(char chineseNumber : chineseNumbers){
                if(c == chineseNumber){
                    isChineseNumber = true;
                    break;
                }
            }
            if(!isChineseNumber){
                return false;
            }
        }
        //指定的字符串已经识别为中文数字串
        //下面要判断中文数字串是否完整
        if(start>0){
            //判断前一个字符，如果为中文数字字符则识别失败
            char c = text.charAt(start-1);
            for(char chineseNumber : chineseNumbers){
                if(c == chineseNumber){
                    return false;
                }
            }
        }
        if(start+len < text.length()){
            //判断后一个字符，如果为中文数字字符则识别失败
            char c = text.charAt(start+len);
            for(char chineseNumber : chineseNumbers){
                if(c == chineseNumber){
                    return false;
                }
            }
        }
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("识别出中文数字：" + text.substring(start, start + len));
        }
        return true;
    }
    public static void main(String[] args){
        String t="0.08%";
        LOGGER.info(""+recog(t, 0, t.length()));
    }
}