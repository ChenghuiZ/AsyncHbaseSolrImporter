/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chhz.persist.solr.thread;

import com.chhz.persist.bean.InfoItf;
import com.chhz.persist.importer.ImporterConnection;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AddSolrByQueryThread implements Runnable, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddSolrByQueryThread.class);
    private final BlockingQueue<Map.Entry<String, Collection<InfoItf>>> solrAddByQueryHbaseQueue;
    private final ImporterConnection importerConnection = new ImporterConnection();

    public AddSolrByQueryThread(BlockingQueue<Map.Entry<String, Collection<InfoItf>>> solrAddByQueryHbaseQueue) {
        this.solrAddByQueryHbaseQueue = solrAddByQueryHbaseQueue;
    }

    @Override
    public void run() {
        while (true) {
            Map.Entry<String, Collection<InfoItf>> entry;
            try {
                entry = solrAddByQueryHbaseQueue.take();
                String clazz = entry.getKey();
                List<Long> ids = entry.getValue().stream().map(infoItf -> infoItf.getPrimaryKey()).collect(Collectors.toList());
                List<InfoItf> queryBeans = importerConnection.getHbaseService(clazz).query(ids);
                if (queryBeans != null && queryBeans.size() > 0) {
                    try {
                        importerConnection.getSolrService(clazz).addDirect(queryBeans);
                    } catch (SolrServerException | IOException e) {
                        LOGGER.error("SolrAddThread tpye {} error：", entry.getKey(), e);
                    }
                }
            } catch (InterruptedException ex) {
                LOGGER.error("Queue take error", ex);
            } catch (Exception ex) {
                LOGGER.error("SolrAddThread error", ex);
            }

        }
    }

}
