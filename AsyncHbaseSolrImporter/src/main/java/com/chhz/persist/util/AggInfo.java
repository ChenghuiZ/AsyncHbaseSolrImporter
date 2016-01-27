/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.chhz.persist.util;

import com.chhz.persist.bean.InfoItf;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

/**
 *
 * @author Run
 */
public class AggInfo {

    private InfoItf bean;
    private Put put;
    private List<Row> rows;

    public AggInfo(InfoItf bean, Put put) {
        this.bean = bean;
        this.put = put;
    }

    public AggInfo(InfoItf bean, List<Row> rows) {
        this.bean = bean;
        this.rows = rows;
    }

    public InfoItf getBean() {
        return bean;
    }

    public void setBean(InfoItf bean) {
        this.bean = bean;
    }

    public Put getPut() {
        return put;
    }

    public void setPut(Put put) {
        this.put = put;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

}
