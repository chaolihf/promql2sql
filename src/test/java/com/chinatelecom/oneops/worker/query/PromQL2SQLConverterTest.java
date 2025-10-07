package com.chinatelecom.oneops.worker.query;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class PromQL2SQLConverterTest { 

    private Map<String, String> metricMap = new HashMap<>(){
        {
            put("container_cpu_usage_seconds_total", "container_pod_cpu_info");
        }
    };

    private Map<String,List<String>> metricLabelMap=new HashMap<>(){
        {
            put("container_pod_cpu_info", Arrays.asList("namespace","pod"));
        }
    };
    

    @Test
    public void testConvert() throws IOException{
        String[] testSources=new String[]{
            "matrix_selector2",
            "matrix_selector1",
            "instant_selector1",
            "instant_selector2",
            "instant_selector3",
            "instant_selector4"
        };
        String[] testResults=new String[]{
            "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime>now()-interval '5 min' group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",            
            "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime>now()-interval '5 min' group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",            
            "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now()-interval '2 min' order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
            "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now()-interval '2 min' order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
            "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now()-interval '2 min' and job='kubelet' order by receivetime desc limit 1) and a0.job='kubelet' group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
            "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now()-interval '2 min' order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
        };
        IMetricFinder finder=new IMetricFinder(){
            public String findTableName(String metricName) {
                return metricMap.get(metricName);
            }
            public List<String> getMetricLabels(String tableName){
                return metricLabelMap.get(tableName);
            } 
        };
        for (int i=0;i<testSources.length;i++) {
            PromQL2SQLConverter converter = new PromQL2SQLConverter(finder);
            String sourceText=loadTestContent(testSources[i]);
            String result=converter.convertPromQL(sourceText);
            assertTrue(String.format("转化文件%s中PromQL语句%s,\n结果是到%s\n期望是%s",testSources[i],sourceText,result,testResults[i]),
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