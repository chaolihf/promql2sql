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
            put("node_network_up","linux_device_info");
            put("node_cpu_seconds_total","linux_cpu_mode_info");
            put("node_load1","linux_info");
        }
    };

    private Map<String,List<String>> metricLabelMap=new HashMap<>(){
        {
            put("container_pod_cpu_info", Arrays.asList("namespace","pod"));
            put("linux_device_info", Arrays.asList("device"));
            put("linux_cpu_mode_info",Arrays.asList("cpu","mode"));
            put("linux_info",Arrays.asList());
        }
    };
    

    @Test
    public void testConvert() throws IOException{
        String[] testSources=new String[]{
            "aggregation5",
            "aggregation3",
            "aggregation2",
            "aggregation1",
            "duration",
            "at_modifier_constant",
            "function3",
            "function2",
            "function1",
            "offset",
            "matrix_selector3",
            "matrix_selector2",
            "matrix_selector1",
            "instant_selector1",
            "instant_selector2",
            "instant_selector3",
            "instant_selector4"
        };
        String[] testResults = new String[] {
                "select a1.receivetime,a1.cpu,a1.mode,last(a1.node_cpu_seconds_total,a1.receivetime) node_cpu_seconds_total from (select a0.receivetime,a0.cpu,a0.mode,last(a0.node_cpu_seconds_total,a0.receivetime) node_cpu_seconds_total from linux_cpu_mode_info a0 where a0.receivetime between now() - interval '1 min' and now() group by a0.receivetime,a0.cpu,a0.mode order by a0.receivetime asc,a0.cpu asc,a0.mode asc) a1 group by a1.receivetime,a1.cpu,a1.mode",
                "select a1.receivetime,a1.node_cpu_seconds_total total_count,a1.mode,count(1) node_cpu_seconds_total from (select a0.receivetime,a0.cpu,a0.mode,last(a0.node_cpu_seconds_total,a0.receivetime) node_cpu_seconds_total from linux_cpu_mode_info a0 where a0.receivetime=(select receivetime from linux_cpu_mode_info where receivetime>now() - interval '2 min' and receivetime<now() order by receivetime desc limit 1) group by a0.receivetime,a0.cpu,a0.mode order by a0.receivetime asc,a0.cpu asc,a0.mode asc) a1 group by a1.receivetime,a1.node_cpu_seconds_total,a1.mode",
                "select a1.receivetime,a1.mode,count(a1.node_cpu_seconds_total) node_cpu_seconds_total from (select a0.receivetime,a0.cpu,a0.mode,last(a0.node_cpu_seconds_total,a0.receivetime) node_cpu_seconds_total from linux_cpu_mode_info a0 where a0.receivetime=(select receivetime from linux_cpu_mode_info where receivetime>now() - interval '2 min' and receivetime<now() and object_id='6cce615e-8afa-4bde-a648-b4163a0ee957' order by receivetime desc limit 1) and a0.object_id='6cce615e-8afa-4bde-a648-b4163a0ee957' group by a0.receivetime,a0.cpu,a0.mode order by a0.receivetime asc,a0.cpu asc,a0.mode asc) a1 group by a1.receivetime,a1.mode",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now() - interval '1 year 5 min 900 millisecond' - interval '2 min' and receivetime<now() - interval '1 year 5 min 900 millisecond' order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.cpu,a0.mode,last(a0.node_cpu_seconds_total,a0.receivetime) node_cpu_seconds_total from linux_cpu_mode_info a0 where a0.receivetime=(select receivetime from linux_cpu_mode_info where receivetime>TO_TIMESTAMP(1609746000) - interval '2 min' and receivetime<TO_TIMESTAMP(1609746000) order by receivetime desc limit 1) group by a0.receivetime,a0.cpu,a0.mode order by a0.receivetime asc,a0.cpu asc,a0.mode asc",
                "select a1.receivetime,clamp(a1.node_load1,0,10) node_load1 from (select a0.receivetime,last(a0.node_load1,a0.receivetime) node_load1 from linux_info a0 where a0.receivetime=(select receivetime from linux_info where receivetime>now() - interval '2 min' and receivetime<now() order by receivetime desc limit 1) group by a0.receivetime order by a0.receivetime asc) a1",
                "select a2.receivetime,a2.cpu,a2.mode,sin(a2.node_cpu_seconds_total) node_cpu_seconds_total from (select a1.receivetime,a1.cpu,a1.mode,safeDiv(a1.node_cpu_seconds_total-lag(a1.node_cpu_seconds_total) over (partition by a1.cpu,a1.mode order by a1.receivetime),extract(epoch from (a1.receivetime-lag(a1.receivetime) over (partition by a1.cpu,a1.mode order by a1.receivetime)))) node_cpu_seconds_total from (select a0.receivetime,a0.cpu,a0.mode,last(a0.node_cpu_seconds_total,a0.receivetime) node_cpu_seconds_total from linux_cpu_mode_info a0 where a0.receivetime between now() - interval '5 min' and now() group by a0.receivetime,a0.cpu,a0.mode order by a0.receivetime asc,a0.cpu asc,a0.mode asc) a1) a2",
                "select a2.device,extract (day from a2.receivetime) receivetime from (select a1.device,a1.receivetime from (select a0.receivetime,a0.device,last(a0.node_network_up,a0.receivetime) node_network_up from linux_device_info a0 where a0.receivetime=(select receivetime from linux_device_info where receivetime>now() - interval '2 min' and receivetime<now() and device='eth0' order by receivetime desc limit 1) and a0.device='eth0' group by a0.receivetime,a0.device order by a0.receivetime asc,a0.device asc) a1) a2",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now() - interval '5 min' - interval '2 min' and receivetime<now() - interval '5 min' order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select time_bucket('1 min',a0.receivetime) receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime between now() - interval '5 min' and now() group by time_bucket('1 min',a0.receivetime),a0.namespace,a0.pod order by time_bucket('1 min',a0.receivetime) asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime between now() - interval '5 min' and now() group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime between now() - interval '5 min' and now() group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now() - interval '2 min' and receivetime<now() order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now() - interval '2 min' and receivetime<now() order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now() - interval '2 min' and receivetime<now() and job='kubelet' order by receivetime desc limit 1) and a0.job='kubelet' group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
                "select a0.receivetime,a0.namespace,a0.pod,last(a0.container_cpu_usage_seconds_total,a0.receivetime) container_cpu_usage_seconds_total from container_pod_cpu_info a0 where a0.receivetime=(select receivetime from container_pod_cpu_info where receivetime>now() - interval '2 min' and receivetime<now() order by receivetime desc limit 1) group by a0.receivetime,a0.namespace,a0.pod order by a0.receivetime asc,a0.namespace asc,a0.pod asc",
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
            String result=converter.convertPromQL(sourceText,"now()","now()");
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