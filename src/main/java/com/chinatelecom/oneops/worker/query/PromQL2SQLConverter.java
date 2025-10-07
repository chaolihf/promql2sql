package com.chinatelecom.oneops.worker.query;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.chinatelecom.oneops.worker.query.generate.PromQLLexer;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ExpressionContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4labelMatcherListContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4metricNameContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.LabelMatcherContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.MatrixSelectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4subqueryContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4vectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParserBaseVisitor;

/**
 * 根据promQL生成SQL
 */
public class PromQL2SQLConverter extends PromQLParserBaseVisitor<SQLToken>{

    private final static String FIELD_TIME="receivetime";
    /**
     * 定义默认延迟的时间为2分钟，超过这个时间的数据不进行查询
     */
    private static final int DELAY_TIME = 2; 
    private IMetricFinder metricFinder;
    private int aliasIndex=0;

    private String getAliasName(){
        return "a"+aliasIndex++;
    }
    
    public PromQL2SQLConverter(IMetricFinder finder){
        this.metricFinder=finder;
    }

    @Override
    public SQLToken visitExpression(ExpressionContext ctx) {
        return visit(ctx.vectorOperation());
    }


    @Override
    public SQLToken visitVectorOperation4vector(VectorOperation4vectorContext ctx) {
        return visit(ctx.vector());
    }

    @Override
    public SQLToken visitInstantSelector4metricName(InstantSelector4metricNameContext ctx) {
        List<String[]> labelMatchers=null;
        if(ctx.labelMatcherList()!=null){
            labelMatchers=new ArrayList<>();
            for(LabelMatcherContext labelMatcher : ctx.labelMatcherList().labelMatcher()){
                labelMatchers.add(new String[]{
                    labelMatcher.labelName().getText(),
                    labelMatcher.labelMatcherOperator().getText(),
                    labelMatcher.STRING().getText()
                });
            }
        } 
        return generateInstantToken(ctx.METRIC_NAME().getText(),labelMatchers);
    }

    @Override
    public SQLToken visitInstantSelector4labelMatcherList(InstantSelector4labelMatcherListContext ctx) {
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
                    labelName,labelMatcher.labelMatcherOperator().getText(),labelMatcher.STRING().getText()
                });
            }
            
        }
        return generateInstantToken(metricName,labelMatchers);
    }

    private SQLToken generateInstantToken(String metricName,List<String[]> labelMatchers){
        SQLToken token = new SQLToken();
        String metricTableName=metricFinder.findTableName(metricName);
        List<String> labels=metricFinder.getMetricLabels(metricTableName);
        String aliasName=getAliasName();
        token.setTableNameWithAlias(metricTableName, aliasName);
        String timeExpression=String.format("%s.%s",aliasName,FIELD_TIME);
        token.addField(timeExpression);
        for(String label:labels){
            token.addField(String.format("%s.%s",aliasName,label));
        }
        token.addField(String.format("last(%s.%s,%s) %s",aliasName,metricName,timeExpression,metricName));
        StringBuffer conditionString=new StringBuffer();
        if(labelMatchers!=null){
            conditionString.append(" and ");
            for(String[] labelMatcher : labelMatchers){
                token.addCondition(String.format("%s.%s%s%s",aliasName,labelMatcher[0],labelMatcher[1],labelMatcher[2]));
                conditionString.append(String.format("%s%s%s",labelMatcher[0],labelMatcher[1],labelMatcher[2])).append(" and ");
            }
            conditionString.delete(conditionString.length()-5,conditionString.length());
        } 
        token.insertCondition(0,String.format("%s=(select %s from %s where %s>now()-interval '%d min'%s order by %s desc limit 1)",
            timeExpression,FIELD_TIME,metricTableName,FIELD_TIME, DELAY_TIME,conditionString.toString(), FIELD_TIME));
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
    public SQLToken visitMatrixSelector(MatrixSelectorContext ctx) {
        SQLToken instanceToken=visit(ctx.instantSelector());
        //删除默认时间查询条件
        instanceToken.removeCondition(0);
        instanceToken.addCondition(String.format("%s.%s>now()-interval '%s'",
            instanceToken.getTableAlias(),FIELD_TIME,getDurationExpression(ctx.TIME_RANGE().getText())));
        return instanceToken;
    }

    @Override
    public SQLToken visitVectorOperation4subquery(VectorOperation4subqueryContext ctx) {
        SQLToken instanceToken=visit(ctx.vectorOperation());
        //删除默认时间查询条件
        instanceToken.removeCondition(0);
        String[] subRange=ctx.subqueryOp().SUBQUERY_RANGE().getText().split(":");
        instanceToken.addCondition(String.format("%s.%s>now()-interval '%s'",
            instanceToken.getTableAlias(),FIELD_TIME,getDurationExpression(subRange[0])));
        if(subRange[1].length()!=0){

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

    public String convertPromQL(String promQL) {
        CharStream input=CharStreams.fromString(promQL);
        PromQLLexer lexer=new PromQLLexer(input);
        CommonTokenStream tokens=new CommonTokenStream(lexer);
        PromQLParser parser=new PromQLParser(tokens);
        ParseTree tree=parser.expression();
        SQLToken result= this.visit(tree);
        return result.getSql();
    }
    
    

}
