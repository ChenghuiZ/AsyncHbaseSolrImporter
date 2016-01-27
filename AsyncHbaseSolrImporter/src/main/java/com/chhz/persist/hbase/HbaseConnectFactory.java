package com.chhz.persist.hbase;

import com.chhz.persist.config.PersistConfigure;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseConnectFactory {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(HbaseConnectFactory.class);

    private volatile static Connection hbaseConnect;

    public static Connection createConnection() {
        if (hbaseConnect == null) {
            synchronized (HbaseConnectFactory.class) {
                if (hbaseConnect == null) {
                    Configuration cfg = new Configuration();
                    cfg.set("hbase.zookeeper.quorum",
                            PersistConfigure.getHbaseZookeeperIPs());
                    //	    cfg.setInt("HBASE_CLIENT_RETRIES_NUMBER", 3);
                    //	    cfg.setInt("START_LOG_ERRORS_AFTER_COUNT_KEY", 2);
                    try {
                        hbaseConnect = ConnectionFactory.createConnection(cfg);
                    } catch (IOException e) {
                        LOGGER.error("获取hbase连接失败。");
                    }
                }
            }
        }
        return hbaseConnect;
    }

}
