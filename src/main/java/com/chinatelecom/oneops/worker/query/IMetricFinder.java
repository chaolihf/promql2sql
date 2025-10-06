package com.chinatelecom.oneops.worker.query;

import java.util.List;

public interface IMetricFinder {

    /**
     * 返回指标对应的数据库表
     * @param metricName
     * @return
     */
    public String findTableName(String metricName);

    /**
     * 返回指标表的标签
     * @param tableName
     * @return
     */
    public List<String> getMetricLabels(String tableName);

}
