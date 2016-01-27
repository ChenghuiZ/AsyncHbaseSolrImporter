package com.chhz.persist.config;

import java.util.Map;
import org.yaml.snakeyaml.Yaml;

public class PersistConfigure {

    private static final int DEFALUT_HBASE_MAX_CACHE_COUNT = 1000; // 缓存大小，当达到该上限时提交
    private static final int DEFALUT_SOLR_COMMIT_WITHIN_MS = 300000;//
    private static final int DEFALUT_SOLR_ADD_MAX_QUEUE_COUNT = 100; // 缓存大小，当达到该上限时提交
    private static final int DEFALUT_SOLR_UPDATE_MAX_QUEUE_COUNT = 100;//
    private static final Map conf;

    static {
        conf = (Map) new Yaml().load(PersistConfigure.class
                .getResourceAsStream("/persist.yml"));
    }

    private PersistConfigure() {
    }

    public static int getHbaseMaxCacheCount() {
        return ((Number) conf.getOrDefault("hbase.maxCacheCount", DEFALUT_HBASE_MAX_CACHE_COUNT)).intValue();
    }

    public static String getHbaseZookeeperIPs() {
        return (String) conf.get("hbase.zkHosts");
    }

    public static int getSolrCommitWithinMs() {
        return ((Number) conf.getOrDefault("solr.commitWithinMs", DEFALUT_SOLR_COMMIT_WITHIN_MS)).intValue();
    }

    public static int getSolrAddMaxQueueCount() {
        return ((Number) conf.getOrDefault("solrAdd.maxQueueCount", DEFALUT_SOLR_ADD_MAX_QUEUE_COUNT)).intValue();
    }

    public static int getSolrUpdateMaxQueueCount() {
        return ((Number) conf.getOrDefault("solrUpdate.maxQueueCount", DEFALUT_SOLR_UPDATE_MAX_QUEUE_COUNT)).intValue();
    }

    public static String getSolrZookeeperIPs() {
        return (String) conf.get("solr.zkHosts");
    }

    public static void init(Map customConf) {
        if (customConf != null) {
            conf.putAll(customConf);
        }
    }

}
