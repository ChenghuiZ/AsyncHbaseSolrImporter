package com.chhz.persist.util;

import com.chhz.persist.annotation.TransitionField;
import com.chhz.persist.annotation.UpdateField;
import com.chhz.persist.constant.EnumFieldUpdateType;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ReflectionCache {

    private static final ConcurrentHashMap<String, Class<?>> clazzCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Map<String, Field>> fieldsCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, MapList<EnumFieldUpdateType, Field>> updateFieldsCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class, List> notUpdatefieldsCache = new ConcurrentHashMap<>();

    public static Class<?> getClass(String className)
            throws ClassNotFoundException {
        Class<?> clazz = clazzCache.get(className);
        if (clazz == null) {
            clazz = Class.forName(className);
            clazzCache.put(className, clazz);
        }
        return clazz;
    }

    public static Map<String, Field> getFieldsMap(Class<?> clazz) {
        if (fieldsCache.get(clazz.getName()) == null) {
            Class<?> tmpclazz = clazz;
            Map<String, Field> innerFieldsMap = new HashMap<>();
            while (tmpclazz != null && tmpclazz != Object.class) {
                Field[] tmpfields = tmpclazz.getDeclaredFields();
                for (Field field : tmpfields) {
                    if (Modifier.isStatic(field.getModifiers()) || field.isAnnotationPresent(TransitionField.class)) {
                        continue;
                    }
                    if (!innerFieldsMap.keySet().contains(field.getName())) {
                        field.setAccessible(true);
                        innerFieldsMap.put(field.getName(), field);
                    }
                }
                tmpclazz = tmpclazz.getSuperclass();
            }
            fieldsCache.put(clazz.getName(), innerFieldsMap);
        }
        return fieldsCache.get(clazz.getName());
    }

    public static Collection<Field> getFields(Class<?> clazz) {
        return ReflectionCache.getFieldsMap(clazz).values();
    }

    public static Field getField(Class<?> clazz, String fieldName) {
        return ReflectionCache.getFieldsMap(clazz).get(fieldName);
    }

    public static MapList<EnumFieldUpdateType, Field> getUpdateFields(
            Class<?> clazz) {
        if (updateFieldsCache.get(clazz.getName()) == null) {
            MapList<EnumFieldUpdateType, Field> mapList = new MapList<>();
            ReflectionCache.getFields(clazz).stream().forEach((field) -> {
                boolean isPresent = field.isAnnotationPresent(UpdateField.class);
                if (isPresent) {
                    // 取注解中的文字说明
                    UpdateField annotation = field
                            .getAnnotation(UpdateField.class);
                    mapList.putValue(annotation.type(), field);
                }
            });
            updateFieldsCache.put(clazz.getName(), mapList);
        }
        return updateFieldsCache.get(clazz.getName());
    }

    public static List<Field> getNotUpdateFields(Class<?> clazz) {
        return notUpdatefieldsCache.computeIfAbsent(clazz, queryClazz -> {
            List nUpdateFields = new ArrayList();
            ReflectionCache.getFields(queryClazz).stream().forEach((field) -> {
                if (!field.isAnnotationPresent(UpdateField.class)) {
                    nUpdateFields.add(field);
                }
            });
            return nUpdateFields;
        });
    }
}
