package org.apdplat.search.service;

import org.apdplat.search.mysql.VisitorSource;
import org.apdplat.search.utils.ConfUtils;
import org.apdplat.search.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by ysc on 1/8/17.
 */
public class SearchService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchService.class);
    private static ShortTextSearcher shortTextSearcher = null;
    private static final int MAX_NGRAM = ConfUtils.getInt("maxNgram", 6);

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

    private static void startupIncrementUpdateIndexScheduler(){
        LocalDateTime current = LocalDateTime.now();
        // 计算现在离明天调度时间差多少秒
        LocalDateTime period = current.plusDays(current.isBefore(current.withHour(3).withMinute(30).withSecond(0)) ? 0 : 1).withHour(3).withMinute(30).withSecond(0);
        LOGGER.info("现在时间: {}", TimeUtils.toString(current));
        LOGGER.info("明天03:30全量重建索引调度时间: {}", TimeUtils.toString(period));
        // 初次执行延迟时间 秒
        long initialDelay = ChronoUnit.SECONDS.between(current, period);
        LOGGER.info("初次执行延迟时间: {}", TimeUtils.getTimeDes(initialDelay*1000));
        SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(()->init(),
                initialDelay,
                24*60*60, TimeUnit.SECONDS);
    }

    public static void init(){
        synchronized (SearchService.class) {
            long start = System.currentTimeMillis();
            LOGGER.info("开始全量重建索引");
            shortTextSearcher.clear();

            VisitorSource.index(LocalDateTime.now().minusYears(15), LocalDateTime.now().plusDays(1), "day");

            LOGGER.info("全量重建索引完成, 耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis() - start));
        }
    }

    private static void startupRealtimeUpdateIndexScheduler(){
        EXECUTOR_SERVICE.submit(()->{
            while(true){
                try{
                    Thread.sleep(60*1000);
                    synchronized (SearchService.class) {
                        LocalDateTime minute = LocalDateTime.now().minusMinutes(1);
                        LOGGER.info("开始构建分钟索引: {}", TimeUtils.toString(minute, "yyyy-MM-dd HH:mm:00"));
                        EXECUTOR_SERVICE.submit(() -> buildLastMinuteIndex(minute));
                    }
                }catch (Exception e){
                    LOGGER.error("分钟索引异常", e);
                }
            }
        });
    }

    private static void buildLastMinuteIndex(LocalDateTime minute) {
        String minuteStr = TimeUtils.toString(minute, "yyyy-MM-dd HH:mm:00");
        LOGGER.info("开始导出数据, minute: {}", minuteStr);
        long ts = System.currentTimeMillis();
        LocalDateTime start = minute;
        LocalDateTime end = minute.plusMinutes(1);
        long s = System.currentTimeMillis();
        int count = VisitorSource.index(start, end, "minute");
        LOGGER.info("构建分钟索引成功, minute: {}, 数据条数: {}, 总耗时: {}",
                TimeUtils.toString(minute, "yyyy-MM-dd HH:mm:00"),
                count,
                TimeUtils.getTimeDes(System.currentTimeMillis()-s));
    }

    public static ShortTextSearcher getShortTextSearcher(){
        return shortTextSearcher;
    }

    public static void startup(){
        LOGGER.info("maxNgram: {}", MAX_NGRAM);
        startupShortTextSearcher();
        LOGGER.info("search service ready!");
        LOGGER.info("start index rebuild scheduler");
        startupIncrementUpdateIndexScheduler();
        LOGGER.info("started index rebuild scheduler, done!");
        LOGGER.info("start index realtime scheduler");
        startupRealtimeUpdateIndexScheduler();
        LOGGER.info("started index realtime scheduler, done!");
    }

    private static void saveIndex(){
        LOGGER.info("start save index");
        long start = System.currentTimeMillis();
        shortTextSearcher.saveIndex();
        LOGGER.info("saved index, cost: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
    }

    private static void startupShortTextSearcher() {
        LOGGER.info("start short text searcher ...");
        shortTextSearcher = new ShortTextSearcher(MAX_NGRAM);
        init();
        LOGGER.info("short text searcher started, done!");
    }

    public static void shutdown() {
        if(shortTextSearcher != null) {
            LOGGER.info("clear short text searcher");
            shortTextSearcher.clear();
        }
        List<Runnable> runnableList = SCHEDULED_EXECUTOR_SERVICE.shutdownNow();
        LOGGER.info("停止任务调度: {}", runnableList.toString());
    }
}