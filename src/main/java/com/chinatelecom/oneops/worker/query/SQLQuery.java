package com.chinatelecom.oneops.worker.query;

import java.util.ArrayList;
import java.util.List;

import com.chinatelecom.oneops.worker.query.entity.ConditionPart;
import com.chinatelecom.oneops.worker.query.entity.FieldPart;
import com.chinatelecom.oneops.worker.query.entity.GroupPart;
import com.chinatelecom.oneops.worker.query.entity.LimitPart;
import com.chinatelecom.oneops.worker.query.entity.OrderPart;
import com.chinatelecom.oneops.worker.query.entity.SubQueryTablePart;
import com.chinatelecom.oneops.worker.query.entity.TablePart;

public class SQLQuery {

    private TablePart tablePart=null;
    private FieldPart timePart=null;
    private List<FieldPart> labelsPart=null;
    private FieldPart metricPart=null;
    private List<ConditionPart> conditionsPart=null;
    private List<OrderPart> ordersPart=null;
    private List<GroupPart> groupsPart=null;
    private LimitPart limitPart=null;
    
    public String getSql() {
        StringBuffer sql=new StringBuffer("select ");
        if(timePart!=null){
            sql.append(timePart.getExpression());
            sql.append(",");
        }
        if(labelsPart!=null){
            for(FieldPart fieldPart:labelsPart){
                sql.append(fieldPart.getExpression());
                sql.append(",");
            }
        }
        if(metricPart!=null){
            sql.append(metricPart.getExpression());
            sql.append(",");
        }
        sql.deleteCharAt(sql.length()-1);
        sql.append(" from ");
        sql.append(tablePart.getSql());
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

    
    public void setTableNameWithQuery(SQLQuery subQuery, String aliasName) {
        tablePart=new SubQueryTablePart(subQuery, aliasName); 
    }

    public String getTableAlias(){
        if(tablePart==null){
            return null;
        }
        return tablePart.getAliasName();
    }

    public void addLabelField(String expression,String fieldName) {
        if (labelsPart==null) {
            labelsPart=new ArrayList<FieldPart>();
        }
        labelsPart.add(new FieldPart(expression,fieldName));
    }

    
    public List<FieldPart> getLabelFields() {
        return labelsPart;
    }

    public void setMetricField(String expression,String fieldName) {
        metricPart=new FieldPart(expression,fieldName);
    }

    
    public FieldPart getMetricField() {
        return metricPart;
    }

    public void setTimeField(String expression,String fieldName) {
        timePart=new FieldPart(expression,fieldName);
    }

    public FieldPart getTimeField() {
        return timePart;
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
        ordersPart.add(new OrderPart(new FieldPart(expression,null), isAsc));
    }

    public OrderPart getOrder(int index) {
        if(ordersPart!=null && index>=0 && index<ordersPart.size()){
            return ordersPart.get(index);
        }
        return null;
    }

    public void addCondition(String condition) {
        if(conditionsPart==null){
            conditionsPart=new ArrayList<ConditionPart>();
        }
        conditionsPart.add(new ConditionPart(condition));
    }
    
    public void insertCondition(int index, String condition) {
        if(conditionsPart==null){
            conditionsPart=new ArrayList<ConditionPart>();
        }
        conditionsPart.add(index, new ConditionPart(condition));
    }

    public ConditionPart getCondition(int index) {
        if(conditionsPart!=null && index>=0 && index<conditionsPart.size()){
            return conditionsPart.get(index);
        }   
        return null;     
    }

    public void addGroup(String group) {
        if(groupsPart==null){
            groupsPart=new ArrayList<GroupPart>();
        }
        groupsPart.add(new GroupPart(group));
    }

    public GroupPart getGroup(int index) {
        if(groupsPart!=null && index>=0 && index<groupsPart.size()){
            return groupsPart.get(index);
        }
        return null;
    }




}
