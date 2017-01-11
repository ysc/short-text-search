package org.apdplat.search.container;

import org.apdplat.search.service.SearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Created by ysc on 1/8/17.
 */
public class SearchServiceListener implements ServletContextListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchServiceListener.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        new Thread(()->SearchService.startup()).start();

    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        SearchService.shutdown();
    }
}