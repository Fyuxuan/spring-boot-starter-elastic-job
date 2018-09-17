package com.yuxuan.elasticjob.spring.boot;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;

import com.dangdang.ddframe.job.api.ElasticJob;
import com.dangdang.ddframe.job.api.JobType;
import com.dangdang.ddframe.job.api.dataflow.DataflowJob;
import com.dangdang.ddframe.job.api.script.ScriptJob;
import com.dangdang.ddframe.job.api.simple.SimpleJob;
import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobTypeConfiguration;
import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.config.script.ScriptJobConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.event.rdb.JobEventRdbConfiguration;
import com.dangdang.ddframe.job.executor.handler.JobProperties.JobPropertiesEnum;
import com.dangdang.ddframe.job.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.spring.api.SpringJobScheduler;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;
import com.yuxuan.elasticjob.spring.boot.annotaion.ElasticJobConfig;
import com.yuxuan.elasticjob.spring.boot.job.db.TimeJob;
import com.yuxuan.elasticjob.spring.boot.job.db.TimeJobDBStorage;


/**
 * 作业任务配置
 * @author yuxuan
 * @version 1.0.0
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass(ElasticJob.class)
@ConditionalOnBean(annotation = ElasticJobConfig.class)
@AutoConfigureAfter(RegistryCenterAutoConfiguration.class)
public class ElasticJobAutoConfiguration {

    @Resource
    private ZookeeperRegistryCenter regCenter;

    @Autowired
    private ApplicationContext applicationContext;

    @PostConstruct
    public void init() throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        //获取数据中的作业记录
        DataSource dataSource = getDataSource();
        TimeJobDBStorage tjDb = new TimeJobDBStorage(dataSource);
        List<TimeJob> timeJobList = tjDb.findTimeJob();
        Map<String, TimeJob> timeJobMap = new HashMap<>();
        for(TimeJob tj : timeJobList) {
            timeJobMap.put(tj.getJobName(), tj);
        }
        //获取作业任务
        Map<String, ElasticJob> elasticJobMap = applicationContext.getBeansOfType(ElasticJob.class);
        
        //循环保存作业
        for(ElasticJob elasticJob : elasticJobMap.values()) {
            Class<? extends ElasticJob> jobClass = elasticJob.getClass();
            if(timeJobMap.get(jobClass.getName()) != null) {//数据库已存在
                continue;
            }
          //获取作业任务注解配置
            ElasticJobConfig elasticJobConfig = jobClass.getAnnotation(ElasticJobConfig.class);
            if(null == elasticJobConfig) {
                continue;
            }
            TimeJob timeJob = new TimeJob();
            timeJob.setJobName(jobClass.getName());
            timeJob.setShardingTotalCount(elasticJobConfig.shardingTotalCount());
            timeJob.setCron(elasticJobConfig.cron());
            timeJob.setFailover(elasticJobConfig.failover()?1:2);
            timeJob.setMisfire(elasticJobConfig.misfire()?1:2);
            timeJob.setJobExceptionHandler(elasticJobConfig.jobExceptionHandler());
            timeJob.setExecutorServiceHandler(elasticJobConfig.jobExceptionHandler());
            timeJob.setStreamingProcess(elasticJobConfig.streamingProcess()?1:2);
            timeJob.setMonitorExecution(elasticJobConfig.monitorExecution()?1:2);
            timeJob.setMonitorPort(elasticJobConfig.monitorPort());
            timeJob.setMaxTimeDiffseconds(elasticJobConfig.maxTimeDiffSeconds());
            timeJob.setReconcileIntervalMinutes(elasticJobConfig.reconcileIntervalMinutes());
            timeJob.setEventTraceRdbDatasource(elasticJobConfig.eventTraceRdbDataSource());
            timeJob.setOverwrite(elasticJobConfig.overwrite()?1:2);
            timeJob.setDisabled(elasticJobConfig.disabled()?1:2);
            timeJob.setStartedTimeoutMilliseconds(elasticJobConfig.startedTimeoutMilliseconds());
            timeJob.setCompletedTimeoutMilliseconds(elasticJobConfig.completedTimeoutMilliseconds());
            timeJob.setShardingItemParameters(elasticJobConfig.shardingItemParameters());
            timeJob.setJobParameter(elasticJobConfig.jobParameter());
            timeJob.setDescription(elasticJobConfig.description());
            timeJob.setScriptCommandLine(elasticJobConfig.scriptCommandLine());
            timeJob.setJobShardingStrategyClass(elasticJobConfig.jobShardingStrategyClass());
            timeJob.setListener(elasticJobConfig.listener().getName());
            timeJob.setDistributedListener(elasticJobConfig.distributedListener().getName());
            tjDb.addTimeJob(timeJob);
        }
        
        //循环解析任务
        for (ElasticJob elasticJob : elasticJobMap.values()) {
            
            Class<? extends ElasticJob> jobClass = elasticJob.getClass();
            TimeJob timeJob = timeJobMap.get(jobClass.getName());
            
            //获取作业任务注解配置
            ElasticJobConfig elasticJobConfig = jobClass.getAnnotation(ElasticJobConfig.class);
            
            //判断数据库是否存在
            if(timeJob != null) {//数据库已存在
                if(timeJob.getValid() == 2 || timeJob.getStatus()==2) { //被删除 或 被禁用
                    continue;
                }
                System.out.println("before ===>"+elasticJobConfig.cron());
                //通过反射把数据库的配置强制设值到注解配置中
                InvocationHandler elasticJobConfigHandler = Proxy.getInvocationHandler(elasticJobConfig);
                Field elasticJobConfigField = elasticJobConfigHandler.getClass().getDeclaredField("memberValues");
                elasticJobConfigField.setAccessible(true);
                Map memberValues = (Map)elasticJobConfigField.get(elasticJobConfigHandler);
                memberValues.put("cron", timeJob.getCron());
                System.out.println("before ===>"+elasticJobConfig.cron());
            }
            
           
            
            
            
            //获取作业类型
            JobType jobType = getJobType(elasticJob);
            //对脚本类型做特殊处理，具体原因请查看：com.dangdang.ddframe.job.executor.JobExecutorFactory.getJobExecutor
            //当获取脚本作业执行器时，ElasticJob实例对象必须为空
            if (Objects.equals(JobType.SCRIPT, jobType)) {
                elasticJob = null;
            }
            //获取Lite作业配置
            LiteJobConfiguration liteJobConfiguration = getLiteJobConfiguration(jobType, jobClass, elasticJobConfig);
            //获取作业事件追踪的数据源配置
            JobEventRdbConfiguration jobEventRdbConfiguration = getJobEventRdbConfiguration(elasticJobConfig.eventTraceRdbDataSource());
            //获取作业监听器
            ElasticJobListener[] elasticJobListeners = creatElasticJobListeners(elasticJobConfig);
            elasticJobListeners = null == elasticJobListeners ? new ElasticJobListener[0] : elasticJobListeners;
            //注册作业
            if (null == jobEventRdbConfiguration) {
                new SpringJobScheduler(elasticJob, regCenter, liteJobConfiguration, elasticJobListeners).init();
            } else {
                new SpringJobScheduler(elasticJob, regCenter, liteJobConfiguration, jobEventRdbConfiguration, elasticJobListeners).init();
            }
        }
    }

    /**
     * 获取作业事件追踪的数据源配置
     *
     * @param eventTraceRdbDataSource 作业事件追踪的数据源Bean引用
     * @return JobEventRdbConfiguration
     */
    private JobEventRdbConfiguration getJobEventRdbConfiguration(String eventTraceRdbDataSource) {
        if (StringUtils.isBlank(eventTraceRdbDataSource)) {
            return null;
        }
        if (!applicationContext.containsBean(eventTraceRdbDataSource)) {
            throw new RuntimeException("not exist datasource [" + eventTraceRdbDataSource + "] !");
        }
        DataSource dataSource = (DataSource) applicationContext.getBean(eventTraceRdbDataSource);
        return new JobEventRdbConfiguration(dataSource);
    }
    
    /**
     * 获取数据源
     * 
     * @return
     */
    private DataSource getDataSource(){
        String dataSourceStr = "dataSource";
        if (!applicationContext.containsBean(dataSourceStr)) {
            throw new RuntimeException("not exist datasource [" + dataSourceStr + "] !");
        }
        DataSource dataSource = (DataSource) applicationContext.getBean(dataSourceStr);
        return dataSource;
    }

    /**
     * 获取作业任务类型
     *
     * @param elasticJob 作业任务
     * @return JobType
     */
    private JobType getJobType(ElasticJob elasticJob) {
        if (elasticJob instanceof SimpleJob) {
            return JobType.SIMPLE;
        } else if (elasticJob instanceof DataflowJob) {
            return JobType.DATAFLOW;
        } else if (elasticJob instanceof ScriptJob) {
            return JobType.SCRIPT;
        } else {
            throw new RuntimeException("unknown JobType [" + elasticJob.getClass() + "]!");
        }
    }

    /**
     * 构建任务核心配置
     *
     * @param jobName          任务执行名称
     * @param elasticJobConfig 任务配置
     * @return JobCoreConfiguration
     */
    private JobCoreConfiguration getJobCoreConfiguration(String jobName, ElasticJobConfig elasticJobConfig) {
        JobCoreConfiguration.Builder builder = JobCoreConfiguration.newBuilder(jobName, elasticJobConfig.cron(), elasticJobConfig.shardingTotalCount())
                .shardingItemParameters(elasticJobConfig.shardingItemParameters())
                .jobParameter(elasticJobConfig.jobParameter())
                .failover(elasticJobConfig.failover())
                .misfire(elasticJobConfig.misfire())
                .description(elasticJobConfig.description());
        if (StringUtils.isNotBlank(elasticJobConfig.jobExceptionHandler())) {
            builder.jobProperties(JobPropertiesEnum.JOB_EXCEPTION_HANDLER.getKey(), elasticJobConfig.jobExceptionHandler());
        }
        if (StringUtils.isNotBlank(elasticJobConfig.executorServiceHandler())) {
            builder.jobProperties(JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER.getKey(), elasticJobConfig.executorServiceHandler());
        }
        return builder.build();
    }

    /**
     * 构建Lite作业
     *
     * @param jobType          任务类型
     * @param jobClass         任务执行类
     * @param elasticJobConfig 任务配置
     * @return LiteJobConfiguration
     */
    private LiteJobConfiguration getLiteJobConfiguration(final JobType jobType, final Class<? extends ElasticJob> jobClass, ElasticJobConfig elasticJobConfig) {

        //构建核心配置
        JobCoreConfiguration jobCoreConfiguration = getJobCoreConfiguration(jobClass.getName(), elasticJobConfig);

        //构建任务类型配置
        JobTypeConfiguration jobTypeConfiguration = getJobTypeConfiguration(jobCoreConfiguration, jobType, jobClass.getCanonicalName(),
                elasticJobConfig.streamingProcess(), elasticJobConfig.scriptCommandLine());

        //构建Lite作业
        return LiteJobConfiguration.newBuilder(Objects.requireNonNull(jobTypeConfiguration))
                .monitorExecution(elasticJobConfig.monitorExecution())
                .monitorPort(elasticJobConfig.monitorPort())
                .maxTimeDiffSeconds(elasticJobConfig.maxTimeDiffSeconds())
                .jobShardingStrategyClass(elasticJobConfig.jobShardingStrategyClass())
                .reconcileIntervalMinutes(elasticJobConfig.reconcileIntervalMinutes())
                .disabled(elasticJobConfig.disabled())
                .overwrite(elasticJobConfig.overwrite()).build();

    }

    /**
     * 获取任务类型配置
     *
     * @param jobCoreConfiguration 作业核心配置
     * @param jobType              作业类型
     * @param jobClass             作业类
     * @param streamingProcess     是否流式处理数据
     * @param scriptCommandLine    脚本型作业执行命令行
     * @return JobTypeConfiguration
     */
    private JobTypeConfiguration getJobTypeConfiguration(JobCoreConfiguration jobCoreConfiguration, JobType jobType,
                                                         String jobClass, boolean streamingProcess, String scriptCommandLine) {
        switch (jobType) {
            case DATAFLOW:
                return new DataflowJobConfiguration(jobCoreConfiguration, jobClass, streamingProcess);
            case SCRIPT:
                return new ScriptJobConfiguration(jobCoreConfiguration, scriptCommandLine);
            case SIMPLE:
            default:
                return new SimpleJobConfiguration(jobCoreConfiguration, jobClass);
        }
    }

    /**
     * 获取监听器
     *
     * @param elasticJobConfig 任务配置
     * @return ElasticJobListener[]
     */
    private ElasticJobListener[] creatElasticJobListeners(ElasticJobConfig elasticJobConfig) {
        List<ElasticJobListener> elasticJobListeners = new ArrayList<>(2);

        //注册每台作业节点均执行的监听
        ElasticJobListener elasticJobListener = creatElasticJobListener(elasticJobConfig.listener());
        if (null != elasticJobListener) {
            elasticJobListeners.add(elasticJobListener);
        }

        //注册分布式监听者
        AbstractDistributeOnceElasticJobListener distributedListener = creatAbstractDistributeOnceElasticJobListener(elasticJobConfig.distributedListener(),
                elasticJobConfig.startedTimeoutMilliseconds(), elasticJobConfig.completedTimeoutMilliseconds());
        if (null != distributedListener) {
            elasticJobListeners.add(distributedListener);
        }

        if (CollectionUtils.isEmpty(elasticJobListeners)) {
            return null;
        }

        //集合转数组
        ElasticJobListener[] elasticJobListenerArray = new ElasticJobListener[elasticJobListeners.size()];
        for (int i = 0; i < elasticJobListeners.size(); i++) {
            elasticJobListenerArray[i] = elasticJobListeners.get(i);
        }
        return elasticJobListenerArray;
    }

    /**
     * 创建每台作业节点均执行的监听
     *
     * @param listener 监听者
     * @return ElasticJobListener
     */
    private ElasticJobListener creatElasticJobListener(Class<? extends ElasticJobListener> listener) {
        //判断是否配置了监听者
        if (listener.isInterface()) {
            return null;
        }
        //判断监听者是否已经在spring容器中存在
        if (applicationContext.containsBean(listener.getSimpleName())) {
            return applicationContext.getBean(listener.getSimpleName(), ElasticJobListener.class);
        }
        //不存在则创建并注册到Spring容器中
        return registerElasticJobListener(listener);
    }

    /**
     * 创建分布式监听者到spring容器
     *
     * @param distributedListener          监听者
     * @param startedTimeoutMilliseconds   最后一个作业执行前的执行方法的超时时间 单位：毫秒
     * @param completedTimeoutMilliseconds 最后一个作业执行后的执行方法的超时时间 单位：毫秒
     * @return AbstractDistributeOnceElasticJobListener
     */
    private AbstractDistributeOnceElasticJobListener creatAbstractDistributeOnceElasticJobListener(Class<? extends AbstractDistributeOnceElasticJobListener> distributedListener,
                                                                                                   long startedTimeoutMilliseconds,
                                                                                                   long completedTimeoutMilliseconds) {
        //判断是否配置了监听者
        if (Objects.equals(distributedListener, AbstractDistributeOnceElasticJobListener.class)) {
            return null;
        }
        //判断监听者是否已经在spring容器中存在
        if (applicationContext.containsBean(distributedListener.getSimpleName())) {
            return applicationContext.getBean(distributedListener.getSimpleName(), AbstractDistributeOnceElasticJobListener.class);
        }
        //不存在则创建并注册到Spring容器中
        return registerAbstractDistributeOnceElasticJobListener(distributedListener, startedTimeoutMilliseconds, completedTimeoutMilliseconds);
    }

    /**
     * 注册每台作业节点均执行的监听到spring容器
     *
     * @param listener 监听者
     * @return ElasticJobListener
     */
    private ElasticJobListener registerElasticJobListener(Class<? extends ElasticJobListener> listener) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(listener);
        beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_PROTOTYPE);
        getDefaultListableBeanFactory().registerBeanDefinition(listener.getSimpleName(), beanDefinitionBuilder.getBeanDefinition());
        return applicationContext.getBean(listener.getSimpleName(), listener);
    }

    /**
     * 注册分布式监听者到spring容器
     *
     * @param distributedListener          监听者
     * @param startedTimeoutMilliseconds   最后一个作业执行前的执行方法的超时时间 单位：毫秒
     * @param completedTimeoutMilliseconds 最后一个作业执行后的执行方法的超时时间 单位：毫秒
     * @return AbstractDistributeOnceElasticJobListener
     */
    private AbstractDistributeOnceElasticJobListener registerAbstractDistributeOnceElasticJobListener(Class<? extends AbstractDistributeOnceElasticJobListener> distributedListener,
                                                                                                      long startedTimeoutMilliseconds,
                                                                                                      long completedTimeoutMilliseconds) {
        BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(distributedListener);
        beanDefinitionBuilder.setScope(BeanDefinition.SCOPE_PROTOTYPE);
        beanDefinitionBuilder.addConstructorArgValue(startedTimeoutMilliseconds);
        beanDefinitionBuilder.addConstructorArgValue(completedTimeoutMilliseconds);
        getDefaultListableBeanFactory().registerBeanDefinition(distributedListener.getSimpleName(), beanDefinitionBuilder.getBeanDefinition());
        return applicationContext.getBean(distributedListener.getSimpleName(), distributedListener);
    }

    /**
     * 获取beanFactory
     *
     * @return DefaultListableBeanFactory
     */
    private DefaultListableBeanFactory getDefaultListableBeanFactory() {
        return (DefaultListableBeanFactory) ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
    }

}