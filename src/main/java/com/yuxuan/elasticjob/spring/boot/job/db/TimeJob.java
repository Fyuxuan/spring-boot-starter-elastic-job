package com.yuxuan.elasticjob.spring.boot.job.db;

import lombok.Data;

/**
 * 定时任务
 * @author yuxuan
 * @date 2018年9月17日
 * @project spring-boot-starter-elastic-job
 * @package com.yuxuan.elasticjob.spring.boot.job.db
 * @class TimeJob.java
 * @version [版本号]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
@Data
public class TimeJob {
    private Long id;

    private String jobName;

    private Integer shardingTotalCount;
    
    private String shardingItemParameters;

    private String jobParameter;
    
    private String description;
    
    private String scriptCommandLine;
    
    private String jobShardingStrategyClass;
    
    private String listener;
    
    private String distributedListener;

    private String cron;

    private Integer failover;

    private Integer misfire;

    private String jobExceptionHandler;

    private String executorServiceHandler;

    private Integer streamingProcess;

    private Integer monitorExecution;

    private Integer monitorPort;

    private Integer maxTimeDiffseconds;

    private Integer reconcileIntervalMinutes;

    private String eventTraceRdbDatasource;

    private Integer overwrite;

    private Integer disabled;

    private Long startedTimeoutMilliseconds;

    private Long completedTimeoutMilliseconds;

    private Long valid;

    private Integer status;

    private Long updateTime;

    private Long updater;
    
}
