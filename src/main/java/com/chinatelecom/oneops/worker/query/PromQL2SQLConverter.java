package com.chinatelecom.oneops.worker.query;

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
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4vectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParserBaseVisitor;

/**
 * 根据promQL生成SQL
 */
public class PromQL2SQLConverter extends PromQLParserBaseVisitor<SQLToken>{

    
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
        SQLToken token = new SQLToken();
        String metricName=ctx.METRIC_NAME().getText();
        String matricTableName=metricFinder.find(metricName);
        String aliasName=getAliasName();
        token.setTableNameWithAlias(matricTableName, aliasName);
        token.addField(String.format("%s.%s",aliasName,metricName));
        if(ctx.LEFT_BRACE()==null || ctx.labelMatcherList()==null){
            token.addOrder(String.format("%s.receivetime",aliasName),false);
            token.setLimit(1);
        } else {
            for(LabelMatcherContext labelMatcher : ctx.labelMatcherList().labelMatcher()){
                token.addCondition(String.format("%s.%s%s%s",aliasName,labelMatcher.labelName().getText(),
                    labelMatcher.labelMatcherOperator().getText(),labelMatcher.STRING().getText()));
            }
            token.addOrder(String.format("%s.receivetime",aliasName),false);
            token.setLimit(1);
        }
        return token;
    }

    

    @Override
    public SQLToken visitInstantSelector4labelMatcherList(InstantSelector4labelMatcherListContext ctx) {
        SQLToken token = new SQLToken();
        String aliasName=getAliasName();
        for(LabelMatcherContext labelMatcher : ctx.labelMatcherList().labelMatcher()){
            String labelName=labelMatcher.labelName().getText();
            if ("__name__".equals(labelName)){
                String metricName=labelMatcher.STRING().getText().replace("\'", "");
                String matricTableName=metricFinder.find(metricName);
                token.setTableNameWithAlias(matricTableName, aliasName);
                token.addField(String.format("%s.%s",aliasName,metricName));
            } else {
                token.addCondition(String.format("%s.%s%s%s",aliasName,labelName,
                labelMatcher.labelMatcherOperator().getText(),labelMatcher.STRING().getText()));
            }
            
        }
        token.addOrder(String.format("%s.receivetime",aliasName),false);
        token.setLimit(1);
        return token;
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
