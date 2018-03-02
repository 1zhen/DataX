package com.alibaba.datax.plugin.writer.otswriter.model;


import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

public class OTSPKColumn {
    private String name;
    private PrimaryKeyType type;
    
    public OTSPKColumn(String name, PrimaryKeyType type) {
        this.name = name;
        this.type = type;
    }
    
    public PrimaryKeyType getType() {
        return type;
    }

    public String getName() {
        return name;
    }
    
}
