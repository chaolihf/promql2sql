package com.chinatelecom.oneops.worker.query;

import java.util.ArrayList;
import java.util.List;

import com.chinatelecom.oneops.worker.query.entity.ConditionPart;
import com.chinatelecom.oneops.worker.query.entity.FieldPart;
import com.chinatelecom.oneops.worker.query.entity.TablePart;

public class SQLToken {

    private TablePart tablePart=null;
    private List<FieldPart> fieldsPart=null;
    private List<ConditionPart> conditionsPart=null;
    
    public String getSql() {
        StringBuffer sql=new StringBuffer("select ");
        if(fieldsPart!=null){
            for(FieldPart fieldPart:fieldsPart){
                sql.append(fieldPart.getExpression());
                sql.append(",");
            }
            sql.deleteCharAt(sql.length()-1);
        }
        sql.append(" from ");
        sql.append(tablePart.getMetricName());
        sql.append(" as ");
        sql.append(tablePart.getAliasName());
        if(conditionsPart!=null){
            sql.append(" where ");
            for(ConditionPart conditionPart:conditionsPart){
                sql.append(conditionPart.getCondition());
                sql.append(" and ");
            }
            sql.delete(sql.length()-5,sql.length());
        }
        return sql.toString();
    }

    public void setTableNameWithAlias(String metricName, String aliasName) {
        tablePart=new TablePart(metricName, aliasName);
    }

    public void addField(String expression) {
        if (fieldsPart==null) {
            fieldsPart=new ArrayList<FieldPart>();
        }
        fieldsPart.add(new FieldPart(expression));
    }

}
