package com.chinatelecom.oneops.worker.query;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class PromQL2SQLConverterTest { 

    private Map<String, String> metricMap = new HashMap<>(){
        {
            put("container_cpu_usage_seconds_total", "container_pod_cpu_info");
        }
    };
    

    @Test
    public void testConvert() throws IOException{
        String[] testSources=new String[]{
            "instant_selector1",
            "instant_selector2",
            "instant_selector3",
            "instant_selector4"
        };
        String[] testResults=new String[]{
            "select a0.container_cpu_usage_seconds_total from container_pod_cpu_info a0 order by a0.receivetime desc limit 1",
            "select a0.container_cpu_usage_seconds_total from container_pod_cpu_info a0 order by a0.receivetime desc limit 1",
            "select a0.container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.job='kubelet' order by a0.receivetime desc limit 1",
            "select a0.container_cpu_usage_seconds_total from container_pod_cpu_info a0 order by a0.receivetime desc limit 1",
        };
        IMetricFinder finder=new IMetricFinder(){
            public String find(String metricName) {
                return metricMap.get(metricName);
            }
        };
        for (int i=0;i<testSources.length;i++) {
            PromQL2SQLConverter converter = new PromQL2SQLConverter(finder);
            String sourceText=loadTestContent(testSources[i]);
            String result=converter.convertPromQL(sourceText);
            assertTrue(String.format("转化文件%s中PromQL语句%s到%s",testSources[i],sourceText,result),
                result.equals(testResults[i]));   
        }
    }

    private String loadTestContent(String filePath) throws IOException{
        ClassLoader classLoader = PromQL2SQLConverterTest.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(filePath + ".txt");
        if (inputStream == null) {
            throw new IOException("找不到资源文件: " + filePath);
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            byte[] buffer = new byte[8024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } finally {
            inputStream.close();
        }
        return new String(outputStream.toByteArray(), "UTF-8");
    }
    

}