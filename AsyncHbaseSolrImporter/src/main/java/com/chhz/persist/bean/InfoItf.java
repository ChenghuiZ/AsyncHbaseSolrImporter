/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chhz.persist.bean;

import java.io.Serializable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zhengd
 */
public interface InfoItf extends Serializable {

//    public static final String SPLIT_CHAR = "#";
    /**
     * 列族名
     */
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes("D");

    String getPrimaryKeyName();

    Long getPrimaryKey();

}
