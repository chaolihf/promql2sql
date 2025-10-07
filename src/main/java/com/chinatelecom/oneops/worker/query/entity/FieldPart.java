package com.chinatelecom.oneops.worker.query.entity;

public class FieldPart {

    private String expression;
    
    public FieldPart(String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
}
