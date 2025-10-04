package com.chinatelecom.oneops.worker.query;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class PromQL2SQLConverterTest { 

    @Test
    public void testConvert(){
        PromQL2SQLConverter converter = new PromQL2SQLConverter();
        String result=converter.convertPromQL("sum(rate(nginx_ingress_controller_requests{ingress=\"nginx-ingress-controller\",code=\"200\"}[5m]))");
        assertTrue("".equals(result));
    }


}