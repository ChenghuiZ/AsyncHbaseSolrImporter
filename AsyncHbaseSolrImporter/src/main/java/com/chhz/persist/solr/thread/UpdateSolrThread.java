/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chhz.persist.solr.thread;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.chhz.persist.bean.InfoItf;
import com.chhz.persist.importer.ImporterConnection;

/**
 *
 * @author Run
 */
public class UpdateSolrThread implements Runnable, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateSolrThread.class);
    private final BlockingQueue<Map.Entry<String, Collection<InfoItf>>> updateSolrQueue;
    private final ImporterConnection importerConnection = new ImporterConnection();

    public UpdateSolrThread(BlockingQueue<Map.Entry<String, Collection<InfoItf>>> updateSolrQueue) {
        this.updateSolrQueue = updateSolrQueue;
    }

    @Override
    public void run() {
        while (true) {
            Map.Entry<String, Collection<InfoItf>> entry;
            try {
                entry = updateSolrQueue.take();
                try {
                    importerConnection.getSolrService(entry.getKey()).updateDirect(entry.getValue());
                } catch (IllegalArgumentException | IllegalAccessException | SolrServerException | IOException e) {
                    LOGGER.error("SolrUpdateThread ,tpye {} error：", entry.getKey(), e);
                }
            } catch (InterruptedException ex) {
                LOGGER.error("Queue take error", ex);
            } catch (Exception ex) {
                LOGGER.error("SolrUpdateThread error", ex);
            }

        }
    }

}
