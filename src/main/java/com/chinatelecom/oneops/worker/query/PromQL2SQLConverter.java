package com.chinatelecom.oneops.worker.query;

import com.chinatelecom.oneops.worker.query.generate.PromQLParser.InstantSelectorContext;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import com.chinatelecom.oneops.worker.query.generate.PromQLLexer;
import com.chinatelecom.oneops.worker.query.generate.PromQLParser;
import com.chinatelecom.oneops.worker.query.generate.PromQLParserBaseVisitor;

/**
 * 根据promQL生成SQL
 */
public class PromQL2SQLConverter extends PromQLParserBaseVisitor<String>{

    @Override
    public String visitInstantSelector(InstantSelectorContext ctx) {
        return super.visitInstantSelector(ctx);
    }

    public String convertPromQL(String promQL) {
        CharStream input=CharStreams.fromString(promQL);
        PromQLLexer lexer=new PromQLLexer(input);
        CommonTokenStream tokens=new CommonTokenStream(lexer);
        PromQLParser parser=new PromQLParser(tokens);
        ParseTree tree=parser.expression();
        return this.visit(tree);
    }
    
    

}
