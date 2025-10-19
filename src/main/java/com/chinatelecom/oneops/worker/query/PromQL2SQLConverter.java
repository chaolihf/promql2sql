package com.chinatelecom.oneops.worker.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.chinatelecom.oneops.worker.query.entity.ConditionPart;
import com.chinatelecom.oneops.worker.query.entity.FieldPart;
import com.chinatelecom.oneops.worker.query.entity.JoinPart.JOIN_METHOD;
import com.chinatelecom.oneops.worker.query.generate.PromQLLexer;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.AddOpContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.AggregationContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.CompareOpContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ExpressionContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.Function_Context;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.GroupingContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4labelMatcherListContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4metricNameContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.LabelMatcherContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.LabelNameContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.LabelNameListContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.LiteralContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.MatrixSelectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.MultOpContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.OffsetContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ParameterContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ParameterListContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ParensContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4addContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4atContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4compareContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4multContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4powContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4subqueryContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4unaryContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4vectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperationContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParserBaseVisitor;

/**
 * 根据promQL生成SQL
 */
public class PromQL2SQLConverter extends PromQLParserBaseVisitor<SQLQuery>{

    private final static String FIELD_TIME="receivetime";
    /**
     * 定义默认延迟的时间为2分钟，超过这个时间的数据不进行查询
     */
    private static final int DELAY_TIME = 2; 

    private static final String PLACEHOLDER_START_TIME="${start_time}";

    private static final String PLACEHOLDER_END_TIME="${end_time}";

    private IMetricFinder metricFinder;
    private int aliasIndex=0;

    private String getAliasName(){
        return "a"+aliasIndex++;
    }
    
    public PromQL2SQLConverter(IMetricFinder finder){
        this.metricFinder=finder;
    }

    @Override
    public SQLQuery visitExpression(ExpressionContext ctx) {
        return visit(ctx.vectorOperation());
    }


    @Override
    public SQLQuery visitVectorOperation4vector(VectorOperation4vectorContext ctx) {
        return visit(ctx.vector());
    }

    @Override
    public SQLQuery visitInstantSelector4metricName(InstantSelector4metricNameContext ctx) {
        List<String[]> labelMatchers=null;
        if(ctx.labelMatcherList()!=null){
            labelMatchers=new ArrayList<>();
            for(LabelMatcherContext labelMatcher : ctx.labelMatcherList().labelMatcher()){
                labelMatchers.add(new String[]{
                    labelMatcher.labelName().getText(),
                    labelMatcher.labelMatcherOperator().getText(),
                    formatString(labelMatcher.STRING().getText())
                });
            }
        } 
        return generateInstantToken(ctx.METRIC_NAME().getText(),labelMatchers);
    }

    @Override
    public SQLQuery visitInstantSelector4labelMatcherList(InstantSelector4labelMatcherListContext ctx) {
        List<String[]> labelMatchers=null;
        String metricName=null;
        for(LabelMatcherContext labelMatcher : ctx.labelMatcherList().labelMatcher()){
            String labelName=labelMatcher.labelName().getText();
            if ("__name__".equals(labelName)){
                metricName=labelMatcher.STRING().getText().replaceAll("[\"']", "");
            } else {
                if(labelMatchers==null){
                    labelMatchers=new ArrayList<>();
                }
                labelMatchers.add(new String[]{
                    labelName,labelMatcher.labelMatcherOperator().getText(),formatString(labelMatcher.STRING().getText())
                });
            }
            
        }
        return generateInstantToken(metricName,labelMatchers);
    }

    private String formatString(String value){
        if (value.startsWith("\"")) {
            return String.format("\'%s\'",value.substring(1, value.length()-1));
        }
        return value; 
    }

    private SQLQuery generateInstantToken(String metricName,List<String[]> labelMatchers){
        SQLQuery subQuery = new SQLQuery();
        String metricTableName=metricFinder.findTableName(metricName);
        if(metricTableName==null) {
            return null;
        }
        List<String> labels=metricFinder.getMetricLabels(metricTableName);
        String aliasName=getAliasName();
        subQuery.setTableNameWithAlias(metricTableName, aliasName);
        String timeExpression=String.format("%s.%s",aliasName,FIELD_TIME);
        subQuery.setTimeField(timeExpression,FIELD_TIME);
        if(labels!=null){
            for(String label:labels){
                subQuery.addLabelField(String.format("%s.%s",aliasName,label),label);
            }
        }
        subQuery.setMetricField(String.format("%s.%s %s",aliasName,metricName,metricName),metricName);
        StringBuffer conditionString=new StringBuffer();
        if(labelMatchers!=null){
            conditionString.append(" and ");
            for(String[] labelMatcher : labelMatchers){
                String sqlOperator=convertOperatorToDB(labelMatcher[1]);
                subQuery.addCondition(String.format("%s.%s%s%s",aliasName,labelMatcher[0],sqlOperator,labelMatcher[2]));
                conditionString.append(String.format("%s%s%s",labelMatcher[0],sqlOperator,labelMatcher[2])).append(" and ");
            }
            conditionString.delete(conditionString.length()-5,conditionString.length());
        } 
        subQuery.insertCondition(0,String.format("%s=(select %s from %s where %s>%s - interval '%d min' and %s<%s%s order by %s desc limit 1)",
            timeExpression,FIELD_TIME,metricTableName,FIELD_TIME, PLACEHOLDER_START_TIME,DELAY_TIME, FIELD_TIME,
                PLACEHOLDER_END_TIME,conditionString.toString(),FIELD_TIME));
        subQuery.addOrder(timeExpression, true);
        if(labels!=null){
            for(String label:labels){
                subQuery.addOrder(String.format("%s.%s",aliasName,label),true);
            }
        }
        return subQuery;
    }

    private String convertOperatorToDB(String operator) {
        final Map<String,String> operatorMap=new HashMap<>(){{
            put("=~","~");
            
        }};
        String mapedOperator=operatorMap.get(operator);
        if(mapedOperator!=null){
            return mapedOperator;
        }
        return operator;
    }

    @Override
    public SQLQuery visitMatrixSelector(MatrixSelectorContext ctx) {
        SQLQuery instanceToken=visit(ctx.instantSelector());
        //替换默认时间查询条件
        instanceToken.getCondition(0).setCondition(String.format("%s.%s between %s - interval '%s' and %s",
            instanceToken.getTableAlias(),FIELD_TIME,PLACEHOLDER_START_TIME,
            getDurationExpression(ctx.TIME_RANGE().getText()),PLACEHOLDER_END_TIME
            ));
        return instanceToken;
    }

    @Override
    public SQLQuery visitVectorOperation4subquery(VectorOperation4subqueryContext ctx) {
        SQLQuery subQuery=visit(ctx.vectorOperation());
        //替换默认时间查询条件
        String[] subRange=ctx.subqueryOp().SUBQUERY_RANGE().getText().split(":");
        subQuery.getCondition(0).setCondition(String.format("%s.%s between %s - interval '%s' and %s",
        subQuery.getTableAlias(),FIELD_TIME,PLACEHOLDER_START_TIME,
            getDurationExpression(subRange[0]),PLACEHOLDER_END_TIME));
        if(subRange[1].length()>1){
            SQLQuery parentQuery=new SQLQuery();
            String aliasName=getAliasName();
            parentQuery.setTableNameWithQuery(subQuery,aliasName);
            String bucketDuration=String.format("time_bucket('%s',%s.%s)",
                getDurationExpression(subRange[1]),aliasName,subQuery.getTimeField().getFieldName());
            parentQuery.setTimeField(String.format("%s %s",bucketDuration,FIELD_TIME),FIELD_TIME);
            String metricName=subQuery.getMetricField().getFieldName();
            if(subQuery.getLabelFields()!=null){
                for(FieldPart labelField:subQuery.getLabelFields()){
                    String labelFieldName=String.format("%s.%s",aliasName,labelField.getFieldName());
                    parentQuery.addLabelField(labelFieldName,labelField.getFieldName());
                }
            }
            parentQuery.setMetricField(String.format("%s.%s %s",aliasName,metricName,metricName),metricName);
            return parentQuery;
        }
        return subQuery;
    }

    private String getDurationExpression(String time) {
        StringBuffer duration = new StringBuffer();
        int i=0;
        while (i < time.length()) {
            char c = time.charAt(i);
            if(c=='[' || c==']'){
                i++;
                continue;
            }
            if (Character.isDigit(c) || c=='-') {
                duration.append(c);
            } else {
                duration.append(" ");
                switch (c) {
                    case 'y':
                        duration.append("year ");
                        break;
                    case 'w':
                        duration.append("week ");
                        break;
                    case 'd':
                        duration.append("day ");
                        break;
                    case 'h':
                        duration.append("hour ");
                        break;
                    case 's':
                        duration.append("second ");
                        break;
                    case 'm':
                        if (i+1<time.length()){
                            if (time.charAt(i+1)=='s'){
                                duration.append("millisecond ");
                                i++;;
                            } else{
                                duration.append("min ");
                            }
                        } else{
                            duration.append("min ");
                        }
                    default:
                        break;
                }
            }
            i++;
        }
        return duration.substring(0, duration.length()-1);
    }

    @Override
    public SQLQuery visitOffset(OffsetContext ctx) {
        SQLQuery subQuery=null;
        if (ctx.instantSelector()!=null){
            subQuery=visit(ctx.instantSelector());
        } else{
            subQuery=visit(ctx.matrixSelector());
        }
        String offsetDuration=getDurationExpression(ctx.DURATION().getText());
        ConditionPart timeCondition=subQuery.getCondition(0);
        String offsetCondition=timeCondition.getCondition()
            .replace(PLACEHOLDER_START_TIME, String.format("%s - interval '%s'",PLACEHOLDER_START_TIME, offsetDuration))
            .replace(PLACEHOLDER_END_TIME, String.format("%s - interval '%s'",PLACEHOLDER_END_TIME, offsetDuration))
            ;
        timeCondition.setCondition(offsetCondition);
        return subQuery;
    }

    @Override
    public SQLQuery visitFunction_(Function_Context ctx) {
        String functionName=ctx.FUNCTION().getText();
        final Map<String,String> mathMap=new HashMap<>(){{
            put("abs","abs");put("ceil","ceil");put("exp","exp");put("floor","floor");put("ln","ln");put("log2","log2");
            put("log10","log10");put("round","round"); put("sgn","sign");put("sqrt","sqrt");put("acos","");
            put("acosh","acosh");put("asin","asin");put("asinh","asinh");put("atan","atan");put("atanh","atan");put("cos","cos");
            put("cosh","cosh");put("sin","sin");put("sinh","sinh");put("tan","tang");put("tanh","tanh");
            put("max_over_time","max");put("avg_over_time","avg");put("min_over_time","min");put("sum_over_time","sum");
            put("count_over_time","count");put("stddev_over_time","stddev");put("stdvar_over_time","stddev_pop");
            
        }};
        if(mathMap.containsKey(functionName)){
            return visitFunc4Math(ctx.parameter(0),mathMap.get(functionName));
        }
        switch (functionName) {
            case "timestamp":{
                return visitFuncTimestamp(ctx.parameter(0));
            }
            case "day_of_month":{
                return visitFuncDayOfMonth(ctx.parameter(0));
            }
            case "rate":{
                return visitFuncRate(ctx.parameter(0));
            }
            case "clamp":{
                return visitFuncClamp(ctx.parameter(0), ctx.parameter(1).getText(),ctx.parameter(2).getText());
            }
            case "last_over_time":{
                return visitFunc4Last(ctx.parameter(0));
            }
            default:
                break;
        }
        return null;
    }

    private SQLQuery visitFunc4Last(ParameterContext parameterContext){
        SQLQuery subQuery=null;
        if(parameterContext.vectorOperation()!=null){
            subQuery=visit(parameterContext.vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        parentQuery.addGroup(timeField);

        String metricName=subQuery.getMetricField().getFieldName();

        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                String labelFieldName=String.format("%s.%s",aliasName,labelField.getFieldName());
                parentQuery.addLabelField(labelFieldName,labelField.getFieldName());
                parentQuery.addGroup(labelFieldName);
            }
        }
        parentQuery.setMetricField(String.format("last(%s.%s,%s) %s",aliasName,metricName,timeField,metricName),metricName);
        return parentQuery;
    }

    private SQLQuery visitFuncClamp(ParameterContext parameterContext,String lowValue,String highValue){
        SQLQuery subQuery=null;
        if(parameterContext.vectorOperation()!=null){
            subQuery=visit(parameterContext.vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                parentQuery.addLabelField(String.format("%s.%s",aliasName,labelField.getFieldName()),labelField.getFieldName());
            }
        }
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("clamp(%s.%s,%s,%s) %s", aliasName, metricName,lowValue,highValue, metricName),metricName);
        return parentQuery;
    }


    private SQLQuery visitFuncTimestamp(ParameterContext parameterContext){
        SQLQuery subQuery=null;
        if(parameterContext.vectorOperation()!=null){
            subQuery=visit(parameterContext.vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                parentQuery.addLabelField(String.format("%s.%s",aliasName,labelField.getFieldName()),labelField.getFieldName());
            }
        }
        String metricName=subQuery.getTimeField().getFieldName();
        parentQuery.setMetricField(String.format("%s.%s", aliasName, metricName),metricName);
        return parentQuery;
    }

    private SQLQuery visitFuncDayOfMonth(ParameterContext parameterContext){
        SQLQuery subQuery=null;
        if(parameterContext.vectorOperation()!=null){
            subQuery=visit(parameterContext.vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                parentQuery.addLabelField(String.format("%s.%s",aliasName,labelField.getFieldName()),labelField.getFieldName());
            }
        }
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("extract (day from %s.%s) %s", aliasName, metricName,metricName),metricName);
        return parentQuery;
    }

    private SQLQuery visitFunc4Math(ParameterContext parameterContext,String mathName){
        SQLQuery subQuery=null;
        if(parameterContext.vectorOperation()!=null){
            subQuery=visit(parameterContext.vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                parentQuery.addLabelField(String.format("%s.%s",aliasName,labelField.getFieldName()),labelField.getFieldName());
            }
        }
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("%s(%s.%s) %s", mathName,aliasName, metricName,metricName),metricName);
        return parentQuery;
    }

    private SQLQuery visitFuncRate(ParameterContext parameterContext){
        SQLQuery subQuery=null;
        if(parameterContext.vectorOperation()!=null){
            subQuery=visit(parameterContext.vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        StringBuffer labelFieldString=new StringBuffer();
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                String labelString= String.format("%s.%s",aliasName,labelField.getFieldName());
                parentQuery.addLabelField(labelString,labelField.getFieldName());
                labelFieldString.append(labelString).append(",");
            }
            if(labelFieldString.length()>0){
                labelFieldString.insert(0, "partition by ");
                labelFieldString.deleteCharAt(labelFieldString.length()-1).append(" ");
            }
        }
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricField=String.format("%s.%s",aliasName,subQuery.getMetricField().getFieldName());
        ;
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format(
            "safeDiv(%s-lag(%s) over (%sorder by %s),extract(epoch from (%s-lag(%s) over (%sorder by %s)))) %s",
                metricField,metricField,labelFieldString.toString(),timeField,timeField,timeField,
                labelFieldString.toString(),timeField,metricName),
            metricName);
        return parentQuery;
    }

    @Override
    public SQLQuery visitVectorOperation4at(VectorOperation4atContext ctx) {
        SQLQuery subQuery=visit(ctx.vectorOperation(0));
        VectorContext modifierContext = (VectorContext) ctx.vectorOperation(1).getChild(0);
        if(modifierContext.literal()!=null){
            String fixedModifier=modifierContext.literal().getText();
            ConditionPart timeCondition=subQuery.getCondition(0);
            String newTime=String.format("TO_TIMESTAMP(%s)",fixedModifier);
            String offsetCondition=timeCondition.getCondition().replace(PLACEHOLDER_START_TIME, newTime)
                .replace(PLACEHOLDER_END_TIME,newTime );
            timeCondition.setCondition(offsetCondition);
        }
        return subQuery;
    }

    @Override
    public SQLQuery visitAggregation(AggregationContext ctx) {
        String aggrOperator=ctx.AGGREGATION_OPERATOR().getText();
        List<String> labelNames=null;
        LabelNameListContext labelNameListCtx=null;
        boolean isWithBy=true;
        if( ctx.by()!=null){
            labelNameListCtx=ctx.by().labelNameList();
        } else if (ctx.without()!=null){
            labelNameListCtx=ctx.without().labelNameList();
            isWithBy=false;
        }
        if(labelNameListCtx!=null){
            labelNames=new ArrayList<>();
            for(LabelNameContext lableNameContext:labelNameListCtx.labelName()){
                labelNames.add(lableNameContext.getText());
            }
        }
        
        switch (aggrOperator) {
            case "sum":
            case "min":
            case "max":
            case "avg":
            case "count":
            case "stddev":
                return visitAggregationSingleParam(aggrOperator,ctx.parameterList().parameter(0),isWithBy,labelNames);
            case "stdvar":
                return visitAggregationSingleParam("stddev_pop",ctx.parameterList().parameter(0),isWithBy,labelNames);
            case "group":
                return visitAggregationGroup(ctx.parameterList().parameter(0),isWithBy,labelNames);
            case "count_values":
                return visitAggregationCountValues(ctx.parameterList(),isWithBy,labelNames);
            case "topk":
               return visitAggregationTopK(false,ctx.parameterList().parameter(0).getText(),ctx.parameterList().parameter(1),isWithBy,labelNames);
            case "bottomk":
                return visitAggregationTopK(true,ctx.parameterList().parameter(0).getText(),ctx.parameterList().parameter(1),isWithBy,labelNames);
            case "quantile":
                return visitAggregationQuantile(ctx.parameterList().parameter(0).getText(),ctx.parameterList().parameter(1),isWithBy,labelNames);
            default:
                break;
        }
        return super.visitAggregation(ctx);
    }

    private SQLQuery visitAggregationSingleParam(String aggrOperator,ParameterContext parameter, boolean isWithBy, List<String> labelNames) {
        SQLQuery subQuery=null;
        if(parameter.vectorOperation()!=null){
            subQuery=visit(parameter.vectorOperation());
        }
        if(subQuery==null){
            return null;
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("%s(%s.%s) %s", aggrOperator,aliasName, metricName,metricName),metricName);
        parentQuery.addGroup(timeField);
        List<String> subQueryLabels=new ArrayList<>();
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                subQueryLabels.add(labelField.getFieldName());
            }
        }
        if(labelNames!=null){
            if (isWithBy){
                for(String labelName:labelNames){
                    if(subQueryLabels.contains(labelName)){
                        String labelField=String.format("%s.%s",aliasName,labelName);
                        parentQuery.addLabelField(labelField,labelName);
                        parentQuery.addGroup(labelField);
                    }
                }
            } else{
                for(String labelName:subQueryLabels){
                    if (!labelNames.contains(labelName)){
                        String labelString= String.format("%s.%s",aliasName,labelName);
                        parentQuery.addLabelField(labelString,labelName);
                        parentQuery.addGroup(labelString);
                    }
                }
            }
        }
        return parentQuery;
    }

    private SQLQuery visitAggregationQuantile(String position,ParameterContext parameter, boolean isWithBy, List<String> labelNames){
        SQLQuery subQuery=null;
        float positionValue=Float.parseFloat(position);
        if(positionValue>1 || positionValue<0){
            return null;
        }
        if(parameter.vectorOperation()!=null){
            subQuery=visit(parameter.vectorOperation());
        }
        if(subQuery==null){
            return null;
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("percentile_cont(%s) within group ( order by %s.%s) %s", 
            position, aliasName, metricName,metricName),metricName);
        parentQuery.addGroup(timeField);
        List<String> subQueryLabels=new ArrayList<>();
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                subQueryLabels.add(labelField.getFieldName());
            }
        }
        if(labelNames!=null){
            if (isWithBy){
                for(String labelName:labelNames){
                    if(subQueryLabels.contains(labelName)){
                        String labelField=String.format("%s.%s",aliasName,labelName);
                        parentQuery.addLabelField(labelField,labelName);
                        parentQuery.addGroup(labelField);
                    }
                }
            } else{
                for(String labelName:subQueryLabels){
                    if (!labelNames.contains(labelName)){
                        String labelString= String.format("%s.%s",aliasName,labelName);
                        parentQuery.addLabelField(labelString,labelName);
                        parentQuery.addGroup(labelString);
                    }
                }
            }
        }
        return parentQuery;
    }

    
    private SQLQuery visitAggregationTopK(boolean isAsc,String top,ParameterContext parameter, boolean isWithBy, List<String> labelNames){
        SQLQuery subQuery=null;
        if(parameter.vectorOperation()!=null){
            subQuery=visit(parameter.vectorOperation());
        }
        if(subQuery==null){
            return null;
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("%s.%s %s", aliasName, metricName,metricName),metricName);
        List<String> subQueryLabels=new ArrayList<>();
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                String labelName=labelField.getFieldName();
                subQueryLabels.add(labelName);
                parentQuery.addLabelField(String.format("%s.%s",aliasName,labelName),labelName);
            }
        }
        String orderMetricName=String.format("%s.%s", aliasName,metricName);
        if(isWithBy && (labelNames==null || labelNames.size()==0)){
            parentQuery.addOrder(orderMetricName, isAsc);
            parentQuery.setLimit(Integer.parseInt(top));
            return parentQuery;
        } else{
            String helpFieldName= metricName + "_order";
            StringBuffer windowSql=new StringBuffer("row_number() over (partition by ");
            windowSql.append(timeField).append(",");
            if(labelNames!=null){
                if (isWithBy){
                    for(String labelName:labelNames){
                        if(subQueryLabels.contains(labelName)){
                            windowSql.append(String.format("%s.%s",aliasName,labelName)).append(",");
                        }
                    }
                } else{
                    for(String labelName:subQueryLabels){
                        if (!labelNames.contains(labelName)){
                            windowSql.append(String.format("%s.%s",aliasName,labelName)).append(",");
                        }
                    }
                }
            }
            windowSql.deleteCharAt(windowSql.length()-1).append(" order by ").append(orderMetricName)
                .append(isAsc?" asc":" desc").append(") ").append(helpFieldName);
            parentQuery.addHelpField(windowSql.toString(),helpFieldName);
            SQLQuery topQuery=new SQLQuery();
            String topAliasName=getAliasName();
            topQuery.setTableNameWithQuery(parentQuery,topAliasName);
            String topTimeField=String.format("%s.%s" ,topAliasName, parentQuery.getTimeField().getFieldName());
            topQuery.setTimeField(topTimeField, parentQuery.getTimeField().getFieldName());
            topQuery.setMetricField(String.format("%s.%s %s", topAliasName, metricName,metricName),metricName);
            for(String labelName:subQueryLabels){
                topQuery.addLabelField(String.format("%s.%s",topAliasName,labelName),labelName);
            }
            topQuery.addCondition(String.format("%s.%s<=%s",topAliasName,helpFieldName,top));
            return topQuery;
        }
    }

    private SQLQuery visitAggregationGroup(ParameterContext parameter, boolean isWithBy, List<String> labelNames) {
        SQLQuery subQuery=null;
        if(parameter.vectorOperation()!=null){
            subQuery=visit(parameter.vectorOperation());
        }
        if(subQuery==null){
            return null;
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("1 %s", metricName),metricName);
        parentQuery.addGroup(timeField);
        if(labelNames!=null){
            if (isWithBy){
                for(String labelName:labelNames){
                    String labelField=String.format("%s.%s",aliasName,labelName);
                    parentQuery.addLabelField(labelField,labelName);
                    parentQuery.addGroup(labelField);
                }
            } else{
                for(FieldPart labelField:subQuery.getLabelFields()){
                    String labelName=labelField.getFieldName();
                    if (!labelNames.contains(labelName)){
                        String labelString= String.format("%s.%s",aliasName,labelName);
                        parentQuery.addLabelField(labelString,labelName);
                        parentQuery.addGroup(labelString);
                    }
                }
            }
        }
        return parentQuery;
    }

    private SQLQuery visitAggregationCountValues(ParameterListContext parameters, boolean isWithBy, List<String> labelNames) {
        SQLQuery subQuery=null;
        String metriValueLabel=parameters.parameter(0).literal().getText().replaceAll("[\"']", "");
        if(parameters.parameter(1).vectorOperation()!=null){
            subQuery=visit(parameters.parameter(1).vectorOperation());
        }
        SQLQuery parentQuery=new SQLQuery();
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField("count(1) " + metricName,metricName);
        parentQuery.addLabelField(String.format("%s.%s %s",aliasName,metricName,metriValueLabel),metriValueLabel);
        parentQuery.addGroup(timeField);
        parentQuery.addGroup(String.format("%s.%s" ,aliasName,metricName));
        if(labelNames!=null){
            if (isWithBy){
                for(String labelName:labelNames){
                    String labelField=String.format("%s.%s",aliasName,labelName);
                    parentQuery.addLabelField(labelField,labelName);
                    parentQuery.addGroup(labelField);
                }
            } else{
                for(FieldPart labelField:subQuery.getLabelFields()){
                    String labelName=labelField.getFieldName();
                    if (!labelNames.contains(labelName)){
                        String labelString= String.format("%s.%s",aliasName,labelName);
                        parentQuery.addLabelField(labelString,labelName);
                        parentQuery.addGroup(labelString);
                    }
                }
            }
        }
        return parentQuery;
    }

    

    @Override
    public SQLQuery visitVectorOperation4unary(VectorOperation4unaryContext ctx) {
        SQLQuery subQuery=visit(ctx.vectorOperation());
        if(ctx.unaryOp().ADD()!=null){
            return subQuery;
        }
        FieldPart metricField = subQuery.getMetricField();
        String[] metricParts=splitExpressionAndAlias(metricField.getExpression());
        subQuery.setMetricField(String.format("-(%s) %s",metricParts[0],metricParts[1]), metricField.getFieldName()); 
        return subQuery;
    }

    private String getCompareOperator(CompareOpContext opContext){
        if(opContext.LE()!=null){
            return opContext.LE().getText();
        } else if (opContext.DEQ()!=null){
            return opContext.DEQ().getText();
        } else if (opContext.NE()!=null){
            return opContext.NE().getText();
        }else if (opContext.GT()!=null){
            return opContext.GT().getText();
        }else if (opContext.LT()!=null){
            return opContext.LT().getText();
        }else {
            return opContext.GE().getText();
        }
    }

    @Override
    public SQLQuery visitVectorOperation4compare(VectorOperation4compareContext ctx) {
        String operator=getCompareOperator(ctx.compareOp());
        boolean isBool =ctx.compareOp().BOOL()!=null;
        if(operator.equals("==")){
            operator="=";
        }
        SQLQuery leftQuery=visit(ctx.vectorOperation(0));
        SQLQuery rightQuery=visit(ctx.vectorOperation(1));
        SQLQuery parentQuery=new SQLQuery();
        SQLQuery subQuery;
        String numString;
        if(leftQuery.getTableAlias()==null){
            if(rightQuery.getTableAlias()==null){
                parentQuery.setMetricField(String.format(isBool?"((%s)%s(%s))::int":"(%s)%s(%s)",
                    leftQuery.getMetricField().getExpression(), operator,rightQuery.getMetricField().getExpression()),null);
                return parentQuery;
            }else{
                subQuery=rightQuery;
                numString=leftQuery.getMetricField().getExpression();
            }
        } else {
            if(rightQuery.getTableAlias()==null){
                subQuery=leftQuery;
                numString=rightQuery.getMetricField().getExpression();
            } else{
                throw new TranslateRuntimeException("表达式需要有一个数字表达式:"+ctx.getText());            
            }
        }
        String aliasName=getAliasName();
        parentQuery.setTableNameWithQuery(subQuery,aliasName);
        String timeField=String.format("%s.%s" ,aliasName, subQuery.getTimeField().getFieldName());
        parentQuery.setTimeField(timeField, subQuery.getTimeField().getFieldName());
        String metricName=subQuery.getMetricField().getFieldName();
        parentQuery.setMetricField(String.format("%s.%s", aliasName, metricName),metricName);
        if(subQuery.getLabelFields()!=null){
            for(FieldPart labelField:subQuery.getLabelFields()){
                String labelName=labelField.getFieldName();
                String labelString= String.format("%s.%s",aliasName,labelName);
                parentQuery.addLabelField(labelString,labelName);
            }
        }
        parentQuery.addCondition(leftQuery.getTableAlias()!=null?
            String.format(isBool?"(%s.%s %s (%s))::int":"%s.%s %s (%s)",aliasName,metricName,operator,numString) : 
            String.format(isBool?"((%s) %s %s.%s)::int":"(%s) %s %s.%s",numString,operator,aliasName,metricName)
        );
        return parentQuery;
    }

    @Override
    public SQLQuery visitVectorOperation4pow(VectorOperation4powContext ctx) {
        return visitVectorOperation4TwoOperator(ctx.vectorOperation(0),ctx.vectorOperation(1),"^",ctx.powOp().grouping());
    }

    @Override
    public SQLQuery visitVectorOperation4add(VectorOperation4addContext ctx) {
        String operator;
        AddOpContext addOperator = ctx.addOp();
        if(addOperator.ADD()!=null){
            operator=addOperator.ADD().getText();
        } else if(addOperator.SUB()!=null){
            operator=addOperator.SUB().getText();
        } else {
            operator="";
        }
        return visitVectorOperation4TwoOperator(ctx.vectorOperation(0),ctx.vectorOperation(1),operator,ctx.addOp().grouping());
    }

    @Override
    public SQLQuery visitVectorOperation4mult(VectorOperation4multContext ctx) {
        String operator;
        MultOpContext multiOperator = ctx.multOp();
        if(multiOperator.MULT()!=null){
            operator=multiOperator.MULT().getText();
        } else if(multiOperator.DIV()!=null){
            operator=multiOperator.DIV().getText();
        } else if(multiOperator.MOD()!=null){
            operator=multiOperator.MOD().getText();
        } else {
            operator="";
        }
        return visitVectorOperation4TwoOperator(ctx.vectorOperation(0),ctx.vectorOperation(1),operator,ctx.multOp().grouping());
    }

    public SQLQuery visitVectorOperation4TwoOperator(VectorOperationContext leftContext,VectorOperationContext rightContext,
        String operator,GroupingContext groupContext) {
        SQLQuery leftQuery=visit(leftContext);
        SQLQuery rightQuery=visit(rightContext);
        SQLQuery parentQuery=new SQLQuery();
        if(leftQuery.getTableAlias()==null){
            if(rightQuery.getTableAlias()==null){
                parentQuery.setMetricField(String.format("(%s)%s(%s)",
                    leftQuery.getMetricField().getExpression(), operator,rightQuery.getMetricField().getExpression()),null);
            }else{
                FieldPart rightMetricField = rightQuery.getMetricField();
                String[] rightMetricParts=splitExpressionAndAlias(rightMetricField.getExpression());
                rightQuery.setMetricField(String.format("(%s)%s(%s) %s",leftQuery.getMetricField().getExpression(),
                    operator,rightMetricParts[0],rightMetricParts[1]), rightMetricField.getFieldName()); 
                return rightQuery;
            }
        } else {
            if(rightQuery.getTableAlias()==null){
                FieldPart leftMetricField = leftQuery.getMetricField();
                String[] leftMetricParts=splitExpressionAndAlias(leftMetricField.getExpression());
                leftQuery.setMetricField(String.format("(%s)%s(%s) %s",leftMetricParts[0],operator,
                    rightQuery.getMetricField().getExpression(),leftMetricParts[1]), leftMetricField.getFieldName()); 
                return leftQuery;
            } else{
                if((leftQuery.getTimeField()!=null) != (rightQuery.getTimeField()!=null)){
                    throw new TranslateRuntimeException("表达式两边时间字段不匹配，存在空值:"+leftContext.parent.getText());
                }
                if ( (leftQuery.getLabelFields()==null) != (rightQuery.getLabelFields()==null)){
                    throw new TranslateRuntimeException("表达式两边标签字段不匹配，存在空值:"+leftContext.parent.getText());
                }
                boolean leftIsMainTable=groupContext==null || groupContext.groupLeft()!=null;
                StringBuffer joinCondition=new StringBuffer();
                String leftAliasName=getAliasName();
                String rightAliasName=getAliasName();
                SQLQuery mainQuery=leftIsMainTable?leftQuery:rightQuery;
                String mainAliasName=leftIsMainTable?leftAliasName:rightAliasName;
                if(groupContext==null){   
                    if(leftQuery.getLabelFields()!=null){
                        Set<String> leftLabels=extractFieldLabels(leftQuery);
                        Set<String> rightLabels=extractFieldLabels(rightQuery);
                        if(!leftLabels.equals(rightLabels)){
                            throw new TranslateRuntimeException("表达式两边标签字段不匹配，字段值不一致:"+leftContext.parent.getText());    
                        }
                    }
                } else{
                    if (groupContext.on_()!=null){
                        Set<String> labels=extractLabelNameListLabels(groupContext.on_().labelNameList());
                        for(String fieldName:  labels){
                            joinCondition.append(leftAliasName).append(".").append(fieldName).append("=")
                                .append(rightAliasName).append(".").append(fieldName).append(" and ");
                        }
                    } else if(groupContext.ignoring()!=null){
                        Set<String> leftLabels=extractFieldLabels(leftQuery);
                        Set<String> rightLabels=extractFieldLabels(rightQuery);
                        if(!leftLabels.equals(rightLabels)){
                            throw new TranslateRuntimeException("表达式两边标签字段不匹配，字段值不一致:"+leftContext.parent.getText());    
                        }
                        Set<String> ignoreLabels=extractLabelNameListLabels(groupContext.ignoring().labelNameList());
                        leftLabels.removeAll(ignoreLabels);
                        for(String fieldName:  leftLabels){
                            joinCondition.append(leftAliasName).append(".").append(fieldName).append("=")
                                .append(rightAliasName).append(".").append(fieldName).append(" and ");
                        }
                    }
                }
                parentQuery.setTableNameWithQuery(mainQuery,mainAliasName);
                if(mainQuery.getTimeField()!=null){
                    String fieldName=mainQuery.getTimeField().getFieldName();
                    parentQuery.setTimeField(String.format("%s.%s",mainAliasName,fieldName), fieldName);
                    joinCondition.append(leftAliasName).append(".").append(fieldName).append("=")
                        .append(rightAliasName).append(".").append(fieldName).append(" and ");
                }
                if(mainQuery.getLabelFields()!=null){
                    for(FieldPart labelField:mainQuery.getLabelFields()){
                        String fieldName=labelField.getFieldName();
                        parentQuery.addLabelField(String.format("%s.%s",mainAliasName,fieldName),fieldName);
                        if (groupContext==null || (groupContext.on_()==null && groupContext.ignoring()==null)){
                            joinCondition.append(leftAliasName).append(".").append(fieldName).append("=")
                            .append(rightAliasName).append(".").append(fieldName).append(" and ");
                        }  
                    }
                }
                if(groupContext!=null){
                    if (groupContext.groupLeft()!=null){
                        for(String fieldName: extractLabelNameListLabels(groupContext.groupLeft().labelNameList())){
                            parentQuery.addLabelField(String.format("%s.%s",rightAliasName,fieldName),fieldName);  
                        }
                    } else if (groupContext.groupRight()!=null){
                        for(String fieldName: extractLabelNameListLabels(groupContext.groupRight().labelNameList())){
                            parentQuery.addLabelField(String.format("%s.%s",leftAliasName,fieldName),fieldName);  
                        }
                    }
                }
                String metricName=(leftIsMainTable?leftQuery:rightQuery).getMetricField().getFieldName();
                parentQuery.setMetricField(String.format("(%s.%s)%s(%s.%s) %s",leftAliasName,
                leftQuery.getMetricField().getFieldName(), operator,
                rightAliasName, rightQuery.getMetricField().getFieldName(),metricName),metricName);
                if(joinCondition.length()>0){
                    joinCondition.insert(0, " on (");
                    joinCondition.delete(joinCondition.length()-5, joinCondition.length());
                    joinCondition.append(") ");
                }
                parentQuery.addJoinQuery(leftIsMainTable?rightQuery:leftQuery, JOIN_METHOD.INNER_JOIN,joinCondition.toString(),
                    leftIsMainTable?rightAliasName:leftAliasName);                
            }
        }
        return parentQuery;
    }

    private Set<String> extractFieldLabels(SQLQuery query) {
        Set<String> labels=new HashSet<>();
        for(FieldPart labelField:query.getLabelFields()){
            labels.add(labelField.getFieldName());
        }
        return labels;
    }

    private Set<String> extractLabelNameListLabels(LabelNameListContext listContext){
        Set<String> labels=new HashSet<>();
        for(LabelNameContext labeleNameContext:  listContext.labelName()){
            labels.add(labeleNameContext.getText());
        }
        return labels;
    }

    private String[] splitExpressionAndAlias(String expression) {
        int index=expression.lastIndexOf(" ");
        return new String[]{expression.substring(0,index),expression.substring(index+1)};
    }

    @Override
    public SQLQuery visitLiteral(LiteralContext ctx) {
        String literal;
        if(ctx.NUMBER()!=null){
            literal=ctx.NUMBER().getText();
            if(literal.startsWith(".")){
                literal="0"+literal;
            } else if (literal.toLowerCase().startsWith("0x")){
                literal=String.valueOf(Integer.parseInt(literal.substring(2),16)) + "::numeric";
            } else if (literal.toLowerCase().indexOf("inf")!=-1){
                literal=String.format("'%s'::numeric",literal);
            } else if (literal.equalsIgnoreCase("nan")){
                literal="null::numeric";
            } else{
                if(literal.indexOf(".")==-1){
                    literal=literal + "::numeric";
                }
            }
        } else{
            literal=ctx.STRING().getText() + "::numeric";
        }
        SQLQuery query=new SQLQuery();
        query.setMetricField(literal, "");
        return query;
    }

    @Override
    public SQLQuery visitParens(ParensContext ctx) {
        return visit(ctx.vectorOperation());
    }

    public String convertPromQL(String promQL,String startTime,String endTime) {
        CharStream input=CharStreams.fromString(promQL);
        PromQLLexer lexer=new PromQLLexer(input);
        CommonTokenStream tokens=new CommonTokenStream(lexer);
        PromQLParser parser=new PromQLParser(tokens);
        ParseTree tree=parser.expression();
        SQLQuery result= this.visit(tree);
        if(result==null) {
            return null;
        } 
        String withoutDurationSql=result.getSql();
        return withoutDurationSql.replace(PLACEHOLDER_START_TIME,startTime).replace(PLACEHOLDER_END_TIME, endTime);
    }
    
    

}
