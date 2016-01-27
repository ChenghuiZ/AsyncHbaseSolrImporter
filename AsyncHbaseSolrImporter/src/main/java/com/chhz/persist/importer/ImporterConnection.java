/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chhz.persist.importer;

import com.chhz.persist.hbase.HbaseService;
import com.chhz.persist.solr.SolrService;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Run
 */
public class ImporterConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImporterConnection.class);
    private final ConcurrentMap<String, HbaseService> hbaseServices = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SolrService> solrServices = new ConcurrentHashMap<>();

    public HbaseService getHbaseService(String clazzName) {
        if (hbaseServices.get(clazzName) == null) {
            try {
                HbaseService hbaseService = new HbaseService(clazzName);
                hbaseServices.put(clazzName, hbaseService);
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.error("create HbaseService error.", e);
            }
        }
        return hbaseServices.get(clazzName);
    }

    public SolrService getSolrService(String clazzName) {
        if (solrServices.get(clazzName) == null) {
            try {
                SolrService solrService = new SolrService(clazzName);
                solrServices.put(clazzName, solrService);
            } catch (ClassNotFoundException e) {
                LOGGER.error("create SolrService error.", e);
            }
        }
        return solrServices.get(clazzName);
    }
}
