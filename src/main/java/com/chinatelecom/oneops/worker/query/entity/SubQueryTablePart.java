package com.chinatelecom.oneops.worker.query.entity;

import com.chinatelecom.oneops.worker.query.SQLQuery;

public class SubQueryTablePart extends TablePart {

    private SQLQuery sqlQuery;
    
    public SubQueryTablePart(SQLQuery sqlQuery, String aliasName) {
        this.sqlQuery=sqlQuery;
        setAliasName(aliasName);
    }

    @Override
    public String getSql() {
        return String.format("(%s)", sqlQuery.getSql());
    }

    

}
