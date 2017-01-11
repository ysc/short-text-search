package org.apdplat.search.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 配置工具
 *
 * 用法说明:
 *         在类路径下面的 conf.txt 文件中指定本机调试开发环境使用的值
 *         在类路径下面的 conf.production.txt 文件中指定生产环境中使用的值, 这个值会覆盖conf.txt中的同KEY的值, 开发阶段, 这个文件为空位注释掉指定的值
 *         以 # 号开始的行为注释
 *         K 和 V 之间用 = 号连接
 * Created by ysc on 1/8/17.
 */
public class ConfUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfUtils.class);

    private static final Map<String, String> CONF = new HashMap<>();

    public static void set(String key, String value){
        CONF.put(key, value);
    }
    public static boolean getBoolean(String key, boolean defaultValue){
        String value = CONF.get(key) == null ? Boolean.valueOf(defaultValue).toString() : CONF.get(key);
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get config：" + key + "=" + value);
        }
        return value.contains("true");
    }
    public static boolean getBoolean(String key){
        return getBoolean(key, false);
    }
    public static int getInt(String key, int defaultValue){
        int value = CONF.get(key) == null ? defaultValue : Integer.parseInt(CONF.get(key).trim());
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get config：" + key + "=" + value);
        }
        return value;
    }
    public static int getInt(String key){
        return getInt(key, -1);
    }
    public static String get(String key, String defaultValue){
        String value = CONF.get(key) == null ? defaultValue : CONF.get(key);
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get config：" + key + "=" + value);
        }
        return value;
    }
    public static String get(String key){
        String value = CONF.get(key);
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("get config：" + key + "=" + value);
        }
        return value;
    }
    static{
        reload();
    }
    /**
     * 重新加载配置文件
     */
    public static void reload(){
        CONF.clear();
        LOGGER.info("start load config");
        long start = System.currentTimeMillis();
        loadConf("conf.txt");
        loadConf("conf.production.txt");
        checkSystemProperties();
        long cost = System.currentTimeMillis() - start;
        LOGGER.info("config have been loaded, cost " + cost + " ms, config item count: " + CONF.size());
        LOGGER.info("config items:");
        AtomicInteger i = new AtomicInteger();
        for(String key : CONF.keySet()){
            LOGGER.info(i.incrementAndGet() + "、" + key + "=" + CONF.get(key));
        }
    }
    /**
     * 强制覆盖默认配置
     * @param confFile 配置文件路径
     */
    public static void forceOverride(String confFile) {
        File file = new File(confFile);
        try(InputStream in = new FileInputStream(file)){
            LOGGER.info("use config file "+file.getAbsolutePath()+" to override default config");
            loadConf(in);
        } catch (Exception ex) {
            LOGGER.error("force override default failed:", ex);
        }
        int i=1;
        for(String key : CONF.keySet()){
            LOGGER.info((i++)+"、"+key+"="+ CONF.get(key));
        }
    }
    /**
     * 加载配置文件
     * @param confFile 类路径下的配置文件
     */
    private static void loadConf(String confFile) {
        InputStream in = ConfUtils.class.getClassLoader().getResourceAsStream(confFile);
        if(in == null){
            LOGGER.info("can not find config file:"+confFile);
            return;
        }
        LOGGER.info("load config file:"+confFile);
        loadConf(in);
    }
    /**
     * 加载配置文件
     * @param in 文件输入流
     */
    private static void loadConf(InputStream in) {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"))){
            String line;
            while((line = reader.readLine()) != null){
                line = line.trim();
                if("".equals(line) || line.startsWith("#")){
                    continue;
                }
                int index = line.indexOf("=");
                if(index==-1){
                    LOGGER.error("error config:"+line);
                    continue;
                }
                //有K V
                if(index>0 && line.length()>index+1) {
                    String key = line.substring(0, index).trim();
                    String value = line.substring(index + 1, line.length()).trim();
                    CONF.put(key, value);
                }
                //有K无V
                else if(index>0 && line.length()==index+1) {
                    String key = line.substring(0, index).trim();
                    CONF.put(key, "");
                }else{
                    LOGGER.error("error config:"+line);
                }
            }
        } catch (IOException ex) {
            System.err.println("load config failed:"+ex.getMessage());
            throw new RuntimeException(ex);
        }
    }
    /**
     * 使用系统属性覆盖配置文件
     */
    private static void checkSystemProperties() {
        for(String key : CONF.keySet()){
            String value = System.getProperty(key);
            if(value != null){
                CONF.put(key, value);
                LOGGER.info("system property override config:"+key+"="+value);
            }
        }
    }
    public static void main(String[] args){
    }
}