/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chhz.persist.importer;

import com.chhz.persist.bean.InfoItf;
import com.chhz.persist.config.PersistConfigure;
import com.chhz.persist.util.AggInfo;
import com.chhz.persist.util.ConcurrentMapList;
import com.chhz.persist.util.HbaseBeanUtil;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncInfoImporterImpl implements InfoImporterItf {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncInfoImporterImpl.class);
    private final ImporterProxy importerProxy = new ImporterProxy();
    private BlockingQueue<Map.Entry<String, Collection<InfoItf>>> addSolrQueue;
    private BlockingQueue<Map.Entry<String, Collection<InfoItf>>> updateSolrQueue;
    private BlockingQueue<Map.Entry<String, Collection<InfoItf>>> addSolrByQueryQueue;
    private ConcurrentMapList<String, Put> addHbaseCache;
    private ConcurrentMapList<String, Row> updateHbaseCache;
    private ConcurrentMapList<String, AggInfo> addHbaseAddSolrCache;
    private ConcurrentMapList<String, AggInfo> updateHbaseUpdateSolrCache;
    private ConcurrentMapList<String, AggInfo> updateHbaseAddSolrByQueryCache;

    @Override
    public void init(Map customConf) {
        PersistConfigure.init(customConf);
        addHbaseCache = new ConcurrentMapList<>();
        updateHbaseCache = new ConcurrentMapList<>();
        addHbaseAddSolrCache = new ConcurrentMapList<>();
        updateHbaseUpdateSolrCache = new ConcurrentMapList<>();
        updateHbaseAddSolrByQueryCache = new ConcurrentMapList<>();
        addSolrQueue = new LinkedBlockingQueue<>(PersistConfigure.getSolrAddMaxQueueCount());
        updateSolrQueue = new LinkedBlockingQueue<>(PersistConfigure.getSolrAddMaxQueueCount());
        addSolrByQueryQueue = new LinkedBlockingQueue<>(PersistConfigure.getSolrAddMaxQueueCount());
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new CommitTimer(), 10, 60, TimeUnit.SECONDS);
    }

    @Override
    public void addHbase(InfoItf info) {
        String clazz = info.getClass().getName();
        Put put = HbaseBeanUtil.convert2Put(info);
        if (put != null && put.size() > 0) {
            addHbaseCache.putValue(clazz, put);
            List<Put> puts = addHbaseCache.get(clazz);
            if (puts.size() >= PersistConfigure.getHbaseMaxCacheCount()) {
                addHbaseCache.remove(clazz);
                try {
                    importerProxy.addHbase(clazz, puts);
                } catch (IOException e) {
                    LOGGER.error("addOnlyHbase type {} error：", clazz, e);
                }
            }
        }
    }

    @Override
    public void updateHbase(InfoItf info) {
        String clazz = info.getClass().getName();
        List<Row> rows = HbaseBeanUtil.getUpdateRows(info);
        if (rows != null && rows.size() > 0) {
            updateHbaseCache.putValues(clazz, rows);
            List<Row> cacheRows = updateHbaseCache.get(clazz);
            if (cacheRows.size() >= PersistConfigure.getHbaseMaxCacheCount()) {
                updateHbaseCache.remove(clazz);
                try {
                    importerProxy.updateHbase(clazz, cacheRows);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("addOnlyHbase type {} error：", clazz, e);
                }
            }
        }
    }

    @Override
    public void addHbaseAddSolr(InfoItf info) {
        String clazz = info.getClass().getName();
        Put put = HbaseBeanUtil.convert2Put(info);
        if (put != null && put.size() > 0) {
            AggInfo aggInfo = new AggInfo(info, put);
            addHbaseAddSolrCache.putValue(clazz, aggInfo);
            List<AggInfo> aggInfos = addHbaseAddSolrCache.get(clazz);
            if (aggInfos.size() >= PersistConfigure.getHbaseMaxCacheCount()) {
                addHbaseAddSolrCache.remove(clazz);
                try {
                    importerProxy.addHbaseAddSolr(clazz, aggInfos);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("addOnlyHbase type {} error：", clazz, e);
                }
            }
        }
    }

    @Override
    public void updateHbaseUpdateSolr(InfoItf info) {
        String clazz = info.getClass().getName();
        List<Row> rows = HbaseBeanUtil.getUpdateRows(info);
        if (rows != null && rows.size() > 0) {
            AggInfo aggInfo = new AggInfo(info, rows);
            updateHbaseUpdateSolrCache.putValue(clazz, aggInfo);
            List<AggInfo> aggInfos = updateHbaseUpdateSolrCache.get(clazz);
            if (aggInfos.size() >= PersistConfigure.getHbaseMaxCacheCount()) {
                updateHbaseUpdateSolrCache.remove(clazz);
                try {
                    importerProxy.updateHbaseUpdateSolr(clazz, aggInfos);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("addOnlyHbase type {} error：", clazz, e);
                }
            }
        }
    }

    @Override
    public void updateHbaseAddSolrByQuery(InfoItf info) {
        String clazz = info.getClass().getName();
        List<Row> rows = HbaseBeanUtil.getUpdateRows(info);
        if (rows != null && rows.size() > 0) {
            AggInfo aggInfo = new AggInfo(info, rows);
            updateHbaseAddSolrByQueryCache.putValue(clazz, aggInfo);
            List<AggInfo> aggInfos = updateHbaseAddSolrByQueryCache.get(clazz);
            if (aggInfos.size() >= PersistConfigure.getHbaseMaxCacheCount()) {
                updateHbaseAddSolrByQueryCache.remove(clazz);
                try {
                    importerProxy.updateHbaseAddSolrByQuery(clazz, aggInfos);
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("addOnlyHbase type {} error：", clazz, e);
                }
            }
        }
    }

    @Override
    public void flush() {
        importerProxy.flushCache();
    }

    class CommitTimer implements Runnable {

        private final ImporterProxy timerImporterProxy = new ImporterProxy();
        Random random = new Random();

        @Override
        public void run() {
            try {
                Thread.sleep(random.nextInt(10000));
                timerImporterProxy.flushCache();
            } catch (Exception e) {
                LOGGER.error("CommitTimer error", e);
            }
        }
    }

    public class ImporterProxy {
        private final ImporterConnection importerConnection=new ImporterConnection();

        private void addHbase(String clazz, List<Put> puts) throws IOException {
            importerConnection.getHbaseService(clazz).addDirect(puts);
        }

        private void updateHbase(String clazz, List<Row> rows) throws IOException, InterruptedException {
            importerConnection.getHbaseService(clazz).batch(rows);
        }

        private void addHbaseAddSolr(String clazz, List<AggInfo> aggInfos) throws IOException, InterruptedException {
            List<Put> puts = aggInfos.stream().map(cacheAggInfo -> cacheAggInfo.getPut()).collect(Collectors.toList());
            importerConnection.getHbaseService(clazz).addDirect(puts);
            List<InfoItf> infos = aggInfos.stream().map(cacheAggInfo -> cacheAggInfo.getBean()).collect(Collectors.toList());
            this.putInfos2Queue(addSolrQueue, clazz, infos);
        }

        private void updateHbaseUpdateSolr(String clazz, List<AggInfo> aggInfos) throws IOException, InterruptedException {
            List<Row> cacheRows = new LinkedList<>();
            aggInfos.stream().forEach((cacheAggInfo) -> {
                cacheRows.addAll(cacheAggInfo.getRows());
            });
            importerConnection.getHbaseService(clazz).batch(cacheRows);
            List<InfoItf> infos = aggInfos.stream().map(cacheAggInfo -> cacheAggInfo.getBean()).collect(Collectors.toList());
            this.putInfos2Queue(updateSolrQueue, clazz, infos);
        }

        private void updateHbaseAddSolrByQuery(String clazz, List<AggInfo> aggInfos) throws IOException, InterruptedException {
            List<Row> cacheRows = new LinkedList<>();
            aggInfos.stream().forEach((cacheAggInfo) -> {
                cacheRows.addAll(cacheAggInfo.getRows());
            });
            importerConnection.getHbaseService(clazz).batch(cacheRows);
            List<InfoItf> infos = aggInfos.stream().map(cacheAggInfo -> cacheAggInfo.getBean()).collect(Collectors.toList());
            this.putInfos2Queue(addSolrByQueryQueue, clazz, infos);
        }

        private void putInfos2Queue(BlockingQueue<Map.Entry<String, Collection<InfoItf>>> queue, String clazz, Collection<InfoItf> infos) throws InterruptedException {
            Map.Entry<String, Collection<InfoItf>> entry = new AbstractMap.SimpleEntry<>(clazz, infos);
            queue.put(entry);
        }

        private void flushCache() {
            try {
                for (Map.Entry<String, List<Put>> entry : addHbaseCache.entrySet()) {
                    List<Put> puts = addHbaseCache.get(entry.getKey());
                    if (puts != null && puts.size() > 0) {
                        entry.setValue(new LinkedList<>());
                        this.addHbase(entry.getKey(), puts);
                    }
                }
                for (Map.Entry<String, List<Row>> entry : updateHbaseCache.entrySet()) {
                    List<Row> rows = updateHbaseCache.get(entry.getKey());
                    if (rows != null && rows.size() > 0) {
                        entry.setValue(new LinkedList<>());
                        this.updateHbase(entry.getKey(), rows);
                    }
                }
                for (Map.Entry<String, List<AggInfo>> entry : addHbaseAddSolrCache.entrySet()) {
                    List<AggInfo> aggInfos = addHbaseAddSolrCache.get(entry.getKey());
                    if (aggInfos != null && aggInfos.size() > 0) {
                        entry.setValue(new LinkedList<>());
                        this.addHbaseAddSolr(entry.getKey(), aggInfos);
                    }
                }
                for (Map.Entry<String, List<AggInfo>> entry : updateHbaseUpdateSolrCache.entrySet()) {
                    List<AggInfo> aggInfos = updateHbaseUpdateSolrCache.get(entry.getKey());
                    if (aggInfos != null && aggInfos.size() > 0) {
                        entry.setValue(new LinkedList<>());
                        this.updateHbaseUpdateSolr(entry.getKey(), aggInfos);
                    }
                }
                for (Map.Entry<String, List<AggInfo>> entry : updateHbaseAddSolrByQueryCache.entrySet()) {
                    List<AggInfo> aggInfos = updateHbaseAddSolrByQueryCache.get(entry.getKey());
                    if (aggInfos != null && aggInfos.size() > 0) {
                        entry.setValue(new LinkedList<>());
                        this.updateHbaseAddSolrByQuery(entry.getKey(), aggInfos);
                    }
                }
            } catch (IllegalArgumentException | InterruptedException | IOException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        }

    }
}
