package com.chinatelecom.oneops.worker.query;

public class SQLToken {

    private String sql;
    
    public SQLToken(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

}
