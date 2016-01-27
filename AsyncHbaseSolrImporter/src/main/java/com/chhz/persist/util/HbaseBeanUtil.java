package com.chhz.persist.util;

import com.chhz.persist.bean.InfoItf;
import com.chhz.persist.constant.EnumFieldUpdateType;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * hbase对象工具
 */
public class HbaseBeanUtil {

    /**
     * RowKey名称
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseBeanUtil.class);
    /**
     * json转换工具
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 将HBASE返回结果转换为指定BEAN
     *
     * @param <T>
     * @param result
     * @param beanClass
     * @return
     */
    public static <T extends InfoItf> T resultToBean(Result result, Class<T> beanClass) {
        T t;
        try {
            t = beanClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("实例化类" + beanClass.getName() + "失败！", e);
        }
        NavigableMap<byte[], byte[]> valueMap = result.getFamilyMap(InfoItf.COLUMN_FAMILY);
        valueMap.keySet().stream().forEach((column) -> {
            setFieldValue(t, Bytes.toString(column), valueMap.get(column));
        });
        return t;
    }

    /**
     * 向指定的成员变量设置属性
     *
     * @param <T>
     * @param t
     * @param fieldName
     * @param value
     */
    public static <T extends InfoItf> void setFieldValue(T t, String fieldName, byte[] value) {
        if (t == null) {
            throw new NullPointerException("参数t不能为空！");
        }
        if (value == null) {
            return;
        }
        Field field = ReflectionCache.getField(t.getClass(), fieldName);
        if (field == null) {
            LOGGER.error("Set value error,Class:" + t + " isn't exsist.");
            return;
        }
        field.setAccessible(true);
        Class<?> fieldType = field.getType();
        // 根据不同的成员变量类型设置不同类型的值
        try {
            if (fieldType.equals(String.class)) {
                field.set(t, Bytes.toString(value));
            } else if (fieldType.equals(Long.class)) {
                field.set(t, Bytes.toLong(value));
            } else if (fieldType.equals(Integer.class)) {
                field.set(t, Bytes.toInt(value));
            } else if (fieldType.equals(Date.class)) {
                field.set(t, new Date(Bytes.toLong(value)));
            } else if (List.class.isAssignableFrom(fieldType)) {
                // 成员变量为list时,将存储的json
                List<String> list = objectMapper.readValue(
                        Bytes.toString(value),
                        new TypeReference<List<String>>() {
                });
                field.set(t, list);
            } else if (fieldType.equals(Double.class)) {
                field.set(t, Bytes.toDouble(value));
            } else if (fieldType.equals(Float.class)) {
                field.set(t, Bytes.toFloat(value));
            } else {
                field.set(t, Bytes.toString(value));
            }
        } catch (IllegalArgumentException | IllegalAccessException | IOException e) {
            LOGGER.error("Set value error,Type:" + t + ",Field:" + fieldName, e);
        }
    }

    public static <T extends InfoItf> Put convert2Put(T t) {
        Collection<Field> tableFields = ReflectionCache.getFields(t.getClass());
        return HbaseBeanUtil.convert2Put(t, tableFields);
    }

    /**
     * 将对象转换为Put对象
     *
     * @param <T>
     * @param t
     * @param tableFields
     * @return
     */
    public static <T extends InfoItf> Put convert2Put(T t, Collection<Field> tableFields) {
        try {
            byte[] rowkey = Bytes.toBytes(t.getPrimaryKey());
            if (rowkey == null) {
                LOGGER.error("请确保" + t + "的主键字段的值不为空！");
                return null;
            }
            Put put = new Put(rowkey);
            for (Field field : tableFields) {
                byte[] value = field2Bytes(field, t);
                if (value != null && value.length > 0) {
                    put.addColumn(InfoItf.COLUMN_FAMILY,
                            Bytes.toBytes(field.getName()), value);
                }
            }
            if (put.size() > 0) {
                return put;
            }
        } catch (SecurityException | IllegalArgumentException | IllegalAccessException | IOException e) {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    /**
     * 将字段值转化为bytes
     *
     * @param <T>
     * @param field
     * @param t
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static <T extends InfoItf> byte[] field2Bytes(Field field, T t)
            throws IllegalArgumentException, IllegalAccessException,
            JsonGenerationException, JsonMappingException, IOException {
        field.setAccessible(true);
        Object value = field.get(t);

        if (value == null) {
            return null;
        }
        Class<?> fieldType = field.getType();

        if (fieldType.equals(String.class)) {
            return Bytes.toBytes(value.toString());
        } else if (fieldType.equals(Long.class)) {
            return Bytes.toBytes((Long) value);
        } else if (fieldType.equals(Integer.class)) {
            return Bytes.toBytes((Integer) value);
        } else if (fieldType.equals(Date.class)) {
            Date val = (Date) value;
            return Bytes.toBytes(val.getTime());
        } else if (List.class.isAssignableFrom(fieldType)) {
            ObjectMapper mapper = new ObjectMapper();
            return Bytes.toBytes(mapper.writeValueAsString(value));
        } else if (fieldType.equals(Double.class)) {
            return Bytes.toBytes((Double) value);
        } else if (fieldType.equals(Float.class)) {
            return Bytes.toBytes((Float) value);
        } else {
            return Bytes.toBytes(value.toString());
        }
    }

    public static <T extends InfoItf> List<Row> getUpdateRows(T t) {
        MapList<EnumFieldUpdateType, Field> updateFields = ReflectionCache.getUpdateFields(t.getClass());
        List<Row> rowList = new ArrayList<>();
        byte[] rowkey = Bytes.toBytes(t.getPrimaryKey());
        if (rowkey == null) {
            LOGGER.error("更新数据的主键字段值为空！");
            return null;
        }
        if (updateFields.get(EnumFieldUpdateType.REPLACE) != null) {
            try {
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
            } catch (SecurityException | IllegalArgumentException | IllegalAccessException | IOException e) {
                LOGGER.error(e.getMessage());
            }
        }
        if (updateFields.get(EnumFieldUpdateType.INCREASE) != null) {
            Increment inc = new Increment(rowkey);
            updateFields.get(EnumFieldUpdateType.INCREASE).stream().forEach((field) -> {
                try {
                    Long value = field.get(t) == null ? 1L : (Long) field.get(t);
                    inc.addColumn(InfoItf.COLUMN_FAMILY, Bytes.toBytes(field.getName()), value);
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    LOGGER.error(e.getMessage());
                }
            });
            if (inc.size() > 0) {
                rowList.add(inc);
            }
        }
        return rowList;
    }
}
