package com.chinatelecom.oneops.worker.query.entity;

public class TablePart {

    private String tableName;
    private String aliasName;
        
    public TablePart(){

    } 

    public TablePart(String tableName, String aliasName) {
        this.tableName = tableName;
        this.aliasName = aliasName;
    }
    public String getSql() {
        return tableName;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }
}
