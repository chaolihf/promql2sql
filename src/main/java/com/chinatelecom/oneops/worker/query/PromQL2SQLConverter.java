package com.chinatelecom.oneops.worker.query;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.chinatelecom.oneops.worker.query.generate.PromQLLexer;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.ExpressionContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelector4metricNameContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser.VectorOperation4vectorContext;
import com.chinatelecom.oneops.worker.query.generate.PromQLParserBaseVisitor;

/**
 * 根据promQL生成SQL
 */
public class PromQL2SQLConverter extends PromQLParserBaseVisitor<SQLToken>{

    

    @Override
    public SQLToken visitExpression(ExpressionContext ctx) {
        return visit(ctx.vectorOperation());
    }


    @Override
    public SQLToken visitVectorOperation4vector(VectorOperation4vectorContext ctx) {
        SQLToken sqlToken = new SQLToken(ctx.getText());
        return sqlToken;
    }


    @Override
    public SQLToken visitInstantSelector4metricName(InstantSelector4metricNameContext ctx) {
        SQLToken token = new SQLToken("select ");
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
