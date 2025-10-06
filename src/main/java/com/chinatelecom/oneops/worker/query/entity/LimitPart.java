package com.chinatelecom.oneops.worker.query.entity;

public class LimitPart {

    private int limit;
    private int offset;

    public LimitPart() {
        
    }

    public LimitPart(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;    
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

}
