package com.chinatelecom.oneops.worker.query;

import java.util.ArrayList;
import java.util.List;

import com.chinatelecom.oneops.worker.query.entity.ConditionPart;
import com.chinatelecom.oneops.worker.query.entity.FieldPart;
import com.chinatelecom.oneops.worker.query.entity.GroupPart;
import com.chinatelecom.oneops.worker.query.entity.LimitPart;
import com.chinatelecom.oneops.worker.query.entity.OrderPart;
import com.chinatelecom.oneops.worker.query.entity.TablePart;

public class SQLToken {

    private TablePart tablePart=null;
    private List<FieldPart> fieldsPart=null;
    private List<ConditionPart> conditionsPart=null;
    private List<OrderPart> ordersPart=null;
    private List<GroupPart> groupsPart=null;
    private LimitPart limitPart=null;
    
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
        sql.append(" ");
        sql.append(tablePart.getAliasName());
        if(conditionsPart!=null){
            sql.append(" where ");
            for(ConditionPart conditionPart:conditionsPart){
                sql.append(conditionPart.getCondition());
                sql.append(" and ");
            }
            sql.delete(sql.length()-5,sql.length());
        }
        if(groupsPart!=null){
            sql.append(" group by ");
            for(GroupPart groupPart:groupsPart){
                sql.append(groupPart.getGroup());
                sql.append(",");
            }
            sql.delete(sql.length()-1,sql.length());
        }
        if(ordersPart!=null){
            sql.append(" order by ");
            for(OrderPart orderPart:ordersPart){
                sql.append(orderPart.getOrderField().getExpression());
                if(orderPart.isAsc()){
                    sql.append(" asc");
                }else{
                    sql.append(" desc");
                }
                sql.append(",");
            }
            sql.delete(sql.length()-1,sql.length());
        }
        if(limitPart!=null){
            if(limitPart.getLimit()>0){
                sql.append(" limit ").append(limitPart.getLimit());
            }
            if(limitPart.getOffset()>0){
                sql.append(" offset ").append(limitPart.getOffset());
            }
        }
        return sql.toString();
    }

    public void setTableNameWithAlias(String metricName, String aliasName) {
        tablePart=new TablePart(metricName, aliasName);
    }

    public String getTableAlias(){
        return tablePart.getAliasName();
    }

    public void addField(String expression) {
        if (fieldsPart==null) {
            fieldsPart=new ArrayList<FieldPart>();
        }
        fieldsPart.add(new FieldPart(expression));
    }

    public void setLimit(int limit) {
        if (limitPart==null){
            limitPart=new LimitPart();
        }
        limitPart.setLimit(limit);
    }

    
    public void removeLimit() {
        limitPart=null;
    }

    public void addOrder(String expression, boolean isAsc) {
        if (ordersPart==null){
            ordersPart=new ArrayList<OrderPart>();
        }
        ordersPart.add(new OrderPart(new FieldPart(expression), isAsc));
    }

    public void addCondition(String condition) {
        if(conditionsPart==null){
            conditionsPart=new ArrayList<ConditionPart>();
        }
        conditionsPart.add(new ConditionPart(condition));
    }

    public void addGroup(String group) {
        if(groupsPart==null){
            groupsPart=new ArrayList<GroupPart>();
        }
        groupsPart.add(new GroupPart(group));
    }


}
