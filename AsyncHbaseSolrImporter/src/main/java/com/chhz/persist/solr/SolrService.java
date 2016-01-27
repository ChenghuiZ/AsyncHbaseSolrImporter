package com.chhz.persist.solr;

import com.chhz.persist.constant.EnumFieldUpdateType;
import com.chhz.persist.util.MapList;
import com.chhz.persist.util.ReflectionCache;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.chhz.persist.bean.InfoItf;

/**
 * solr索引服务类
 *
 * @param <T>
 * @author chhz
 */
public class SolrService<T extends InfoItf> {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(SolrService.class);
    private static final int DEFALUT_COMMIT_WITHIN_MS = 300000;
    private final SolrClient solrServer;
    private final Class<T> tableClass;
    private final MapList<EnumFieldUpdateType, Field> updateFields;

    public SolrService(String clazzName) throws ClassNotFoundException {
        this.tableClass = (Class<T>) Class.forName(clazzName);
        this.updateFields = ReflectionCache.getUpdateFields(tableClass);
        String collectionName = clazzName;
        this.solrServer = SolrServerFactory.createServer(collectionName);
    }

    /**
     * 将数据列表添加到SOLR
     *
     * @param beans
     * @param commitWithinMs
     * @throws SolrServerException
     * @throws IOException
     */
    public void addDirect(Collection<T> beans, int commitWithinMs)
            throws SolrServerException, IOException {
        solrServer.addBeans(beans, commitWithinMs);
    }

    /**
     * 将数据列表添加到SOLR
     *
     * @param beans 入库对象列表
     * @throws IOException
     * @throws SolrServerException
     */
    public void addDirect(Collection<T> beans) throws SolrServerException,
            IOException {
        solrServer.addBeans(beans);
        // TODO 确认每次提交是否影响效率
//        solrServer.commit();
    }

    /**
     * 将数据列表添加到SOLR,并在默认时间（5分钟）内commit
     *
     * @param beans 入库对象列表
     * @throws IOException
     * @throws SolrServerException
     */
    public void addAndCommitDirect(Collection<T> beans)
            throws SolrServerException, IOException {
        solrServer.addBeans(beans, SolrService.DEFALUT_COMMIT_WITHIN_MS);
    }

    /**
     * Deletes a list of documents by unique ID
     *
     * @param ids
     * @throws SolrServerException
     * @throws IOException
     */
    public void deleteById(List<String> ids) throws SolrServerException, IOException {
        solrServer.deleteById(ids);
    }

    /**
     * 原子更新
     *
     * @param beans
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws SolrServerException
     * @throws IOException
     */
    public void updateDirect(Collection<T> beans) throws IllegalArgumentException, IllegalAccessException, SolrServerException, IOException {
        List<SolrInputDocument> updateList = new ArrayList<>(beans.size());
        for (T bean : beans) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.setField(bean.getPrimaryKeyName(), bean.getPrimaryKey());
            if (updateFields.get(EnumFieldUpdateType.REPLACE) != null) {
                for (Field field : updateFields.get(EnumFieldUpdateType.REPLACE)) {
                    field.setAccessible(true);
                    boolean isPresent = field.isAnnotationPresent(org.apache.solr.client.solrj.beans.Field.class);
                    Object value = field.get(bean);
                    if (isPresent && value != null) {
                        Map<String, Object> fieldModifier = new HashMap<>();
                        fieldModifier.put("set", value);
                        doc.addField(field.getName(), fieldModifier);
                    }
                }
            }
            if (updateFields.get(EnumFieldUpdateType.INCREASE) != null) {
                for (Field field : updateFields.get(EnumFieldUpdateType.INCREASE)) {
                    field.setAccessible(true);
                    boolean isPresent = field.isAnnotationPresent(org.apache.solr.client.solrj.beans.Field.class);
                    Object value = field.get(bean) == null ? 1L : field.get(bean);
                    if (isPresent) {
                        Map<String, Object> fieldModifier = new HashMap<>();
                        fieldModifier.put("inc", value);
                        doc.addField(field.getName(), fieldModifier);
                    }
                }
            }
            updateList.add(doc);
        }
        solrServer.add(updateList);
    }

}
