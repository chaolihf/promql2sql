package com.chinatelecom.oneops.worker.query;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.chinatelecom.oneops.worker.query.generate.PromQLLexer;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ExpressionContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.Function_Context;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4labelMatcherListContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4metricNameContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.LabelMatcherContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.MatrixSelectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.OffsetContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ParameterContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4atContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4subqueryContext;
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
                metricName=labelMatcher.STRING().getText().replace("\'","");
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
        SQLQuery token = new SQLQuery();
        String metricTableName=metricFinder.findTableName(metricName);
        List<String> labels=metricFinder.getMetricLabels(metricTableName);
        String aliasName=getAliasName();
        token.setTableNameWithAlias(metricTableName, aliasName);
        String timeExpression=String.format("%s.%s",aliasName,FIELD_TIME);
        token.setTimeField(timeExpression,FIELD_TIME);
        for(String label:labels){
            token.addLabelField(String.format("%s.%s",aliasName,label),label);
        }
        token.setMetricField(String.format("last(%s.%s,%s) %s",aliasName,metricName,timeExpression,metricName),metricName);
        StringBuffer conditionString=new StringBuffer();
        if(labelMatchers!=null){
            conditionString.append(" and ");
            for(String[] labelMatcher : labelMatchers){
                token.addCondition(String.format("%s.%s%s%s",aliasName,labelMatcher[0],labelMatcher[1],labelMatcher[2]));
                conditionString.append(String.format("%s%s%s",labelMatcher[0],labelMatcher[1],labelMatcher[2])).append(" and ");
            }
            conditionString.delete(conditionString.length()-5,conditionString.length());
        } 
        token.insertCondition(0,String.format("%s=(select %s from %s where %s>%s - interval '%d min' and %s<%s%s order by %s desc limit 1)",
            timeExpression,FIELD_TIME,metricTableName,FIELD_TIME, PLACEHOLDER_START_TIME,DELAY_TIME, FIELD_TIME,
                PLACEHOLDER_END_TIME,conditionString.toString(),FIELD_TIME));
        token.addGroup(timeExpression);
        for(String label:labels){
            token.addGroup(String.format("%s.%s",aliasName,label));
        }  
        token.addOrder(timeExpression, true);
        for(String label:labels){
            token.addOrder(String.format("%s.%s",aliasName,label),true);
        }
        return token;
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
        SQLQuery instanceToken=visit(ctx.vectorOperation());
        //替换默认时间查询条件
        String[] subRange=ctx.subqueryOp().SUBQUERY_RANGE().getText().split(":");
        instanceToken.getCondition(0).setCondition(String.format("%s.%s between %s - interval '%s' and %s",
            instanceToken.getTableAlias(),FIELD_TIME,PLACEHOLDER_START_TIME,
            getDurationExpression(subRange[0]),PLACEHOLDER_END_TIME));
        if(subRange[1].length()>1){
            String bucketDuration=String.format("time_bucket('%s',%s.%s)",
                getDurationExpression(subRange[1]),instanceToken.getTableAlias(),FIELD_TIME);
            instanceToken.setTimeField(String.format("%s %s",bucketDuration,FIELD_TIME),FIELD_TIME);
            instanceToken.getGroup(0).setGroup(bucketDuration);
            instanceToken.getOrder(0).setExpression(bucketDuration,true);
        }
        return instanceToken;
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
            if (Character.isDigit(c)) {
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
        Map<String,String> mathMap=new HashMap<>(){{
            put("abs","abs");put("ceil","ceil");put("exp","exp");put("floor","floor");put("ln","ln");put("log2","log2");
            put("log10","log10");put("round","round"); put("sgn","sign");put("sqrt","sqrt");put("acos","");
            put("acosh","acosh");put("asin","asin");put("asinh","asinh");put("atan","atan");put("atanh","atan");put("cos","cos");
            put("cosh","cosh");put("sin","sin");put("sinh","sinh");put("tan","tang");put("tanh","tanh");
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
            default:
                break;
        }
        return null;
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

    public String convertPromQL(String promQL,String startTime,String endTime) {
        CharStream input=CharStreams.fromString(promQL);
        PromQLLexer lexer=new PromQLLexer(input);
        CommonTokenStream tokens=new CommonTokenStream(lexer);
        PromQLParser parser=new PromQLParser(tokens);
        ParseTree tree=parser.expression();
        SQLQuery result= this.visit(tree);
        String withoutDurationSql=result.getSql();
        return withoutDurationSql.replace(PLACEHOLDER_START_TIME,startTime).replace(PLACEHOLDER_END_TIME, endTime);
    }
    
    

}
