package com.chinatelecom.oneops.worker.query.entity;

public class FieldPart {

    private String expression;
    private String fieldName;
    
    public FieldPart(String expression,String fieldName) {
        this.expression = expression;
        this.fieldName=fieldName;
    }

    public String getExpression() {
        return expression;
    }

    public String getFieldName(){
        return fieldName;
    } 

    public void setExpression(String expression,String fieldName) {
        this.expression = expression;
        this.fieldName=fieldName;
    }
}
