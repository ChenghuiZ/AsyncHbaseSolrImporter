package com.chhz.persist.importer;

import com.chhz.persist.bean.InfoItf;
import java.io.Serializable;
import java.util.Map;

public interface InfoImporterItf extends Serializable {

    void init(Map customConf);

    void addHbase(InfoItf info);

    void updateHbase(InfoItf info);

    void addHbaseAddSolr(InfoItf info);

    void updateHbaseUpdateSolr(InfoItf info);

    void updateHbaseAddSolrByQuery(InfoItf info);

    void flush();
}
