package com.chinatelecom.oneops.worker.query.entity;

public class OrderPart {
    private FieldPart orderField;
    private boolean isAsc;

    public OrderPart(){

    }
    
    public OrderPart(FieldPart orderField, boolean isAsc){
        this.orderField = orderField;
        this.isAsc = isAsc;
    }
    public FieldPart getOrderField() {
        return orderField;
    }

    public void setOrderField(FieldPart orderField) {
        this.orderField = orderField;
    }

    public boolean isAsc() {
        return isAsc;
    }

    public void setAsc(boolean isAsc) {
        this.isAsc = isAsc;
    }

    public void setExpression(String expression,boolean isAsc) {
        this.orderField=new FieldPart(expression);
        this.isAsc=isAsc;
    }


}
