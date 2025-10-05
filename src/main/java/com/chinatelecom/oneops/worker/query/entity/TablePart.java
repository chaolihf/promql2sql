package com.chinatelecom.oneops.worker.query.entity;

public class TablePart {

    private String metricName;
    private String aliasName;
        
    public TablePart(String metricName, String aliasName) {
        this.metricName = metricName;
        this.aliasName = aliasName;
    }
    public String getMetricName() {
        return metricName;
    }

    public String getAliasName() {
        return aliasName;
    }

    
}
