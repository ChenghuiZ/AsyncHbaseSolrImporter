package com.chhz.persist.solr;

import com.chhz.persist.config.PersistConfigure;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

public class SolrServerFactory {

    private volatile static Map<String, CloudSolrClient> solrServerMap = new HashMap<>();

    public static SolrClient createServer(String collectionName) {
	if (solrServerMap.get(collectionName) == null) {
	    synchronized (SolrServerFactory.class) {
		if (solrServerMap.get(collectionName) == null) {
		    CloudSolrClient client = new CloudSolrClient(PersistConfigure.getSolrZookeeperIPs());
		    client.setDefaultCollection(collectionName);
		    client.setRequestWriter(new BinaryRequestWriter());
		    solrServerMap.put(collectionName, client);
		}
	    }
	}
	return solrServerMap.get(collectionName);
    }
}
