package com.chhz.persist.hbase;

import com.chhz.persist.bean.InfoItf;
import com.chhz.persist.constant.EnumFieldUpdateType;
import com.chhz.persist.util.HbaseBeanUtil;
import com.chhz.persist.util.MapList;
import com.chhz.persist.util.ReflectionCache;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hbase入库服务类
 *
 * @param <T>
 * @author chhz
 */
public class HbaseService<T extends InfoItf> {

    public static Connection hbaseConnect;
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseService.class);
    private final Table hTable;

    private final Class<T> tableClass;
    private final Collection<Field> tableFields;
    private final MapList<EnumFieldUpdateType, Field> updateFields;

    public HbaseService(String className) throws IOException, ClassNotFoundException {
        hbaseConnect = HbaseConnectFactory.createConnection();

        this.tableClass = (Class<T>) Class.forName(className);
        this.tableFields = ReflectionCache.getFields(tableClass);
        this.updateFields = ReflectionCache.getUpdateFields(tableClass);
        String tableNameS = className;
        TableName tableName = TableName.valueOf(tableNameS);
        try (Admin hbaseAdmin = hbaseConnect.getAdmin()) {
            if (!hbaseAdmin.tableExists(tableName)) {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                HColumnDescriptor columnFamily = new HColumnDescriptor(InfoItf.COLUMN_FAMILY);
                tableDesc.addFamily(columnFamily);
                tableDesc.setDurability(Durability.ASYNC_WAL);
                if (tableNameS.toUpperCase().contains("STATISTIC")) {
                    hbaseAdmin.createTable(tableDesc);
                } else {
                    int regionServerNum = hbaseAdmin.getClusterStatus().getServersSize();
                    hbaseAdmin.createTable(tableDesc, Bytes.toBytes(1223372036854775807L), Bytes.toBytes(-8223372036854775808L), regionServerNum * 3);
                }
            } else {
                hbaseAdmin.getTableDescriptor(tableName)
                        .setDurability(Durability.ASYNC_WAL);
            }
        } catch (TableExistsException ex) {
            LOGGER.info("{} table has been created", tableNameS, ex);
        }
        hTable = hbaseConnect.getTable(tableName);

    }

    public void addDirect(Collection<T> datas) throws IOException {
        List<Put> putList = datas.stream().map((t) -> HbaseBeanUtil.convert2Put(t, this.tableFields)).filter((put) -> (put != null)).collect(Collectors.toList());
        if (putList.size() > 0) {
            hTable.put(putList);
        }
    }

    public void addDirect(List<Put> putList) throws IOException {
        if (putList.size() > 0) {
            hTable.put(putList);
        }
    }

    /**
     * 根据rowkey
     *
     * @param ids
     * @return
     */
    public List<T> query(List<Long> ids) {
        List<Get> gets = ids.stream().map(id -> new Get(Bytes.toBytes(id))).collect(Collectors.toList());
        List<T> beanList = new ArrayList<>();
        // 获取结果集
        try {
            Result[] results = hTable.get(gets);
            for (Result result : results) {
                beanList.add(HbaseBeanUtil.resultToBean(result, tableClass));
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        return beanList;
    }

    public void updateDirect(Collection<T> datas) throws JsonGenerationException, JsonMappingException,
            IllegalArgumentException, IllegalAccessException, IOException,
            InterruptedException {
        List<Row> rowList = new ArrayList<>();
        for (T t : datas) {
            byte[] rowkey = Bytes.toBytes(t.getPrimaryKey());
            if (rowkey == null) {
                LOGGER.error("更新数据的主键字段值为空！");
                continue;
            }
            if (updateFields.get(EnumFieldUpdateType.REPLACE) != null) {
                Put put = new Put(rowkey);
                for (Field field : updateFields.get(EnumFieldUpdateType.REPLACE)) {
                    byte[] fieldValue = HbaseBeanUtil.field2Bytes(field, t);
                    if (fieldValue != null) {
                        put.addColumn(InfoItf.COLUMN_FAMILY,
                                Bytes.toBytes(field.getName()), fieldValue);
                    }
                }
                if (put.size() > 0) {
                    rowList.add(put);
                }
            }
            if (updateFields.get(EnumFieldUpdateType.INCREASE) != null) {
                Increment inc = new Increment(rowkey);
                for (Field field : updateFields.get(EnumFieldUpdateType.INCREASE)) {
                    Long value = field.get(t) == null ? 1L : (Long) field.get(t);
                    inc.addColumn(InfoItf.COLUMN_FAMILY, Bytes.toBytes(field.getName()), value);
                }
                if (inc.size() > 0) {
                    rowList.add(inc);
                }
            }
        }
        if (rowList.size() > 0) {
            hTable.batch(rowList, new Object[rowList.size()]);
        }
    }

    public void batch(List<Row> rowList) throws IOException, InterruptedException {
        if (rowList.size() > 0) {
            hTable.batch(rowList, new Object[rowList.size()]);
        }
    }
}
