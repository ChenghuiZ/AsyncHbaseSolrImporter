package com.chhz.persist.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * @param <K>
 * @param <T>
 * @author ZhongChenghui
 */
public class MapList<K, T> extends HashMap<K, List<T>> {

    private static final long serialVersionUID = 1L;

    public void putValue(K key, T value) {
        if (this.get(key) == null) {
            List<T> list = new LinkedList<>();
            list.add(value);
            this.put(key, list);
        } else {
            this.get(key).add(value);
        }
    }

    public void putValues(K key, Collection<T> value) {
        if (this.get(key) == null) {
            List<T> list = new LinkedList<>();
            list.addAll(value);
            this.put(key, list);
        } else {
            this.get(key).addAll(value);
        }
    }

}
