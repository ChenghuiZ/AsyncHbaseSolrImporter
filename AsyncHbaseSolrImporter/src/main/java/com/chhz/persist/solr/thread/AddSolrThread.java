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
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AddSolrThread implements Runnable, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddSolrThread.class);
    private final BlockingQueue<Map.Entry<String, Collection<InfoItf>>> addSolrQueue;
    private final ImporterConnection importerConnection = new ImporterConnection();

    public AddSolrThread(BlockingQueue<Map.Entry<String, Collection<InfoItf>>> addSolrQueue) {
        this.addSolrQueue = addSolrQueue;
    }

    @Override
    public void run() {
        while (true) {
            Map.Entry<String, Collection<InfoItf>> entry;
            try {
                entry = addSolrQueue.take();
                try {
                    importerConnection.getSolrService(entry.getKey()).addDirect(entry.getValue());
                } catch (SolrServerException | IOException e) {
                    LOGGER.error("SolrAddThread tpye {} error：", entry.getKey(), e);
                }
            } catch (InterruptedException ex) {
                LOGGER.error("Queue take error", ex);
            } catch (Exception ex) {
                LOGGER.error("SolrAddThread error", ex);
            }

        }
    }

}
