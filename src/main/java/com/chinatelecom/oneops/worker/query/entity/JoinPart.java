package com.chinatelecom.oneops.worker.query.entity;

import com.chinatelecom.oneops.worker.query.SQLQuery;

public class JoinPart {

    public enum JOIN_METHOD{
        INNER_JOIN,LEFT_JOIN,RIGHT_JOIN
    }
    
    private int joinMethod;
    private SQLQuery joinQuery;
    private String joinCondition;
    private String aliasName;

    public JoinPart(){

    }

    public JoinPart(SQLQuery joinQuery,int joinMethod, String joinCondition,String aliasName){
        this.joinQuery=joinQuery;
        this.joinMethod=joinMethod;
        this.joinCondition=joinCondition;
        this.aliasName=aliasName;
    }

    public int getJoinMethod() {
        return joinMethod;
    }

    public void setJoinMethod(int joinMethod) {
        this.joinMethod = joinMethod;
    }

    public SQLQuery getJoinQuery() {
        return joinQuery;
    }

    public void setJoinQuery(SQLQuery joinQuery) {
        this.joinQuery = joinQuery;
    }

    public String getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(String joinCondition) {
        this.joinCondition = joinCondition;
    }

    public String getJoinSQL() {
        StringBuffer sql=new StringBuffer();
        if(joinMethod==JOIN_METHOD.INNER_JOIN.ordinal()){
            sql.append(" INNER JOIN ");
        } else if (joinMethod==JOIN_METHOD.LEFT_JOIN.ordinal()){
            sql.append(" LEFT JOIN ");
        } else if (joinMethod==JOIN_METHOD.RIGHT_JOIN.ordinal()){
            sql.append(" RIGHT JOIN ");
        }
        sql.append("(").append(joinQuery.getSql()).append(") ").append(aliasName).append(joinCondition);
        return sql.toString();
    }

}
