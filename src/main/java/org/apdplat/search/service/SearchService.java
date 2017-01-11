package org.apdplat.search.service;

import org.apdplat.search.utils.ConfUtils;
import org.apdplat.search.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by ysc on 1/8/17.
 */
public class SearchService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchService.class);
    private static ShortTextSearcher shortTextSearcher = null;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static volatile boolean ready = false;
    private static final int MAX_NGRAM = ConfUtils.getInt("maxNgram", 6);

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);

    private static void startupIncrementUpdateIndexScheduler(){
        LocalDateTime current = LocalDateTime.now();
        // 计算现在离明天调度时间差多少秒
        LocalDateTime period = current.plusDays(current.isBefore(current.withHour(3).withMinute(30).withSecond(0)) ? 0 : 1).withHour(3).withMinute(30).withSecond(0);
        LOGGER.info("现在时间: {}", TimeUtils.toString(current));
        LOGGER.info("明天03:30全量重建索引调度时间: {}", TimeUtils.toString(period));
        // 初次执行延迟时间 秒
        long initialDelay = ChronoUnit.SECONDS.between(current, period);
        LOGGER.info("初次执行延迟时间: {}", TimeUtils.getTimeDes(initialDelay*1000));
        SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(()->{
                    long start = System.currentTimeMillis();
                    LOGGER.info("开始全量重建索引");
                    shortTextSearcher.clear();
                    shortTextSearcher.index(ShortTextResource.loadShortText());
                    LOGGER.info("全量重建索引完成, 耗时: {}", TimeUtils.getTimeDes(System.currentTimeMillis()-start));
                },
                initialDelay,
                24*60*60, TimeUnit.SECONDS);
    }

    public static ShortTextSearcher getShortTextSearcher(){
        waitIfNotReady();
        return shortTextSearcher;
    }

    public static boolean waitIfNotReady(){
        if(ready){
            return true;
        }
        try{
            LOGGER.info("waiting for search service...");
            countDownLatch.await();
        }catch (Exception e){
            LOGGER.error("等待搜索服务启动出现异常", e);
        }
        return true;
    }

    public static void startup(){
        LOGGER.info("maxNgram: {}", MAX_NGRAM);
        startupShortTextSearcher();
        ready = true;
        LOGGER.info("search service ready!");
        LOGGER.info("start index rebuild scheduler");
        startupIncrementUpdateIndexScheduler();
        LOGGER.info("started index rebuild scheduler, done!");
        saveIndex();
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
        shortTextSearcher.index(ShortTextResource.loadShortText());
        LOGGER.info("short text searcher started, done!");
        countDownLatch.countDown();
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