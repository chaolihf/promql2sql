package com.chinatelecom.oneops.worker.query;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class PromQL2SQLConverterTest { 

    private Map<String, String> metricMap = new HashMap<>(){
        {
            put("container_cpu_usage_seconds_total", "container_pod_cpu_info");
            put("node_network_up","linux_device_info");
            put("node_cpu_seconds_total","linux_cpu_mode_info");
            put("node_softnet_processed_total","linux_cpu_info");
            put("node_load1","linux_info");
            put("node_os_info","linux_os_version_info");
            put("container_memory_usage_bytes","container_pod_info");
        }
    };

    private Map<String,List<String>> metricLabelMap=new HashMap<>(){
        {
            put("container_pod_cpu_info", Arrays.asList("namespace","pod"));
            put("container_pod_info", Arrays.asList("namespace","pod"));
            put("linux_device_info", Arrays.asList("device"));
            put("linux_cpu_mode_info",Arrays.asList("cpu","mode"));
            put("linux_cpu_info",Arrays.asList("cpu","object_id"));
            put("linux_info",Arrays.asList());
            put("linux_os_version_info",Arrays.asList("name","pretty_name","object_id"));
        }
    };
    
    
    @Test
    public void testConvertCompliance() throws IOException{
        IMetricFinder finder=new IMetricFinder(){
            public String findTableName(String metricName) {
                return metricMap.get(metricName);
            }
            public List<String> getMetricLabels(String tableName){
                return metricLabelMap.get(tableName);
            } 
        };
        String sourceText=loadTestContent("compliance.json");
        JSONArray testDatas=new JSONArray(sourceText);
        for (int i=0;i<testDatas.length();i++) {
            JSONObject testData=testDatas.getJSONObject(i);
            PromQL2SQLConverter converter = new PromQL2SQLConverter(finder);
            String promql=testData.getString("promql");
            if(promql.length()>0){
                String result=converter.convertPromQL(promql,"now()","now()");
                if(result==null){
                    result="";
                }
                if(testData.getString("sql").length()==0){
                    if(result.length()!=0){
                        System.err.printf("未进行测试的语句为%s\n",promql);
                        continue;
                    }
                }
                assertTrue(String.format("转化PromQL语句%s,\n结果是到%s\n期望是%s",
                    promql,result,testData.getString("sql")),result.equals(testData.getString("sql")));  
            } 
        }
    }

    private String loadTestContent(String filePath) throws IOException{
        ClassLoader classLoader = PromQL2SQLConverterTest.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(filePath);
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