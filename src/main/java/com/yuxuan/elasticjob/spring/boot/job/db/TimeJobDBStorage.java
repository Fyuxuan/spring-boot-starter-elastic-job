package com.yuxuan.elasticjob.spring.boot.job.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.sql.DataSource;

import com.dangdang.ddframe.job.event.rdb.DatabaseType;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

/**
 * 定时任务数据库存储与操作.
 * @author yuxuan
 * @date 2018年9月17日
 * @project spring-boot-starter-elastic-job
 * @package com.yuxuan.elasticjob.spring.boot.job.db
 * @class TimeJobDBStorage.java
 * @version [版本号]
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
@Slf4j
public class TimeJobDBStorage {
    
    private static final String TABLE_TIME_JOB = "time_job";
    
    private static final List<String> FIELDS_TIME_JOB = 
            Lists.newArrayList("job_name","cron","valid","status");
    
    private final DataSource dataSource;
    
    private DatabaseType databaseType;
    
    public TimeJobDBStorage(final DataSource dataSource) throws SQLException {
        this.dataSource = dataSource;
        initTables();
    }
    
    private void initTables() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            createTimeJobIfNeeded(conn);
            databaseType = DatabaseType.valueFrom(conn.getMetaData().getDatabaseProductName());
        }
    }
    
    private void createTimeJobIfNeeded(final Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        try (ResultSet resultSet = dbMetaData.getTables(null, null, TABLE_TIME_JOB, new String[]{"TABLE"})) {
            if (!resultSet.next()) {
                createTimeJob(conn);
            }
        }
    }
    
    public List<TimeJob> findTimeJob() throws SQLException {
        List<TimeJob> result = new LinkedList<>();
        Connection conn = dataSource.getConnection();
        PreparedStatement preparedStatement  = conn.prepareStatement(buildSelect(TABLE_TIME_JOB,FIELDS_TIME_JOB));
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            TimeJob timeJob = new TimeJob();
            timeJob.setJobName(resultSet.getString(1));
            timeJob.setCron(resultSet.getString(2));
            timeJob.setValid(resultSet.getLong(3));
            timeJob.setStatus(resultSet.getInt(4));
            result.add(timeJob);
        }
        return result;
    }
    
    
    private String buildSelect(final String tableName, final Collection<String> tableFields) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ");
        for (String each : tableFields) {
            sqlBuilder.append(each).append(",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(" FROM ").append(tableName);
        return sqlBuilder.toString();
    }
    
    public boolean addTimeJob(final TimeJob timeJob) {
        return insertTimeJob(timeJob);
    }
    
    private boolean insertTimeJob(final TimeJob timeJob) {
        boolean result = false;
        String sql = "INSERT INTO `" + TABLE_TIME_JOB + 
                "` ("
                + "`job_name`, "
                + "`sharding_total_count`, "
                + "`cron`, "
                + "`failover`, "
                + "`misfire`, "
                + "`job_exception_handler`, "
                + "`executor_service_handler`, "
                + "`streaming_process`, "
                + "`monitor_execution`, "
                + "`monitor_port`, "
                + "`max_time_diffSeconds`, "
                + "`reconcile_interval_minutes`, "
                + "`event_trace_rdb_dataSource`, "
                + "`overwrite`, "
                + "`disabled`, "
                + "`started_timeout_milliseconds`, "
                + "`completed_timeout_milliseconds`, "
                + "`sharding_item_parameters`, "
                + "`job_parameter`, "
                + "`description`, "
                + "`script_command_line`, "
                + "`job_sharding_strategy_class`, "
                + "`listener`, "
                + "`distributed_listener`"
                + ") "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?);";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
            preparedStatement.setString(1, timeJob.getJobName());
            preparedStatement.setInt(2, timeJob.getShardingTotalCount());
            preparedStatement.setString(3, timeJob.getCron());
            preparedStatement.setInt(4, timeJob.getFailover());
            preparedStatement.setInt(5, timeJob.getMisfire());
            preparedStatement.setString(6, timeJob.getJobExceptionHandler());
            preparedStatement.setString(7, timeJob.getExecutorServiceHandler());
            preparedStatement.setInt(8, timeJob.getStreamingProcess());
            preparedStatement.setInt(9, timeJob.getMonitorExecution());
            preparedStatement.setInt(10, timeJob.getMonitorPort());
            preparedStatement.setInt(11, timeJob.getMaxTimeDiffseconds());
            preparedStatement.setInt(12, timeJob.getReconcileIntervalMinutes());
            preparedStatement.setString(13, timeJob.getEventTraceRdbDatasource());
            preparedStatement.setInt(14, timeJob.getOverwrite());
            preparedStatement.setInt(15, timeJob.getDisabled());
            preparedStatement.setLong(16, timeJob.getStartedTimeoutMilliseconds());
            preparedStatement.setLong(17, timeJob.getCompletedTimeoutMilliseconds());
            preparedStatement.setString(18, timeJob.getShardingItemParameters());
            preparedStatement.setString(19, timeJob.getJobParameter());
            preparedStatement.setString(20, timeJob.getDescription());
            preparedStatement.setString(21, timeJob.getScriptCommandLine());
            preparedStatement.setString(22, timeJob.getJobShardingStrategyClass());
            preparedStatement.setString(23, timeJob.getListener());
            preparedStatement.setString(24, timeJob.getDistributedListener());
            
            preparedStatement.execute();
            result = true;
        } catch (final SQLException ex) {
            if (!isDuplicateRecord(ex)) {
                // TODO 记录失败直接输出日志,未来可考虑配置化
                log.error(ex.getMessage());    
            }
        }
        return result;
    }
    
    /**
     * 创建time_job表
     * 
     * @param conn
     * @throws SQLException
     */
    private void createTimeJob(final Connection conn) throws SQLException {
        String dbSchema = "CREATE TABLE `" + TABLE_TIME_JOB + "` ("
                + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
                + "`job_name` varchar(255) NOT NULL COMMENT '作业名称', "
                + "`sharding_total_count` int(11) DEFAULT NULL COMMENT '作业分片总数', "
                + "`sharding_item_parameters` text, "
                + "`job_parameter` text COMMENT '作业自定义参数', "
                + "`cron` varchar(255) DEFAULT NULL COMMENT 'cron表达式', "
                + "`description` text COMMENT '作业描述', "
                + "`failover` int(1) DEFAULT NULL COMMENT '是否开启任务执行失效转移', "
                + "`misfire` int(4) DEFAULT NULL COMMENT '是否开启错过任务重新执行', "
                + "`job_exception_handler` varchar(255) DEFAULT NULL COMMENT '配置jobProperties定义的枚举控制Elastic-Job的实现细节', "
                + "`executor_service_handler` varchar(255) DEFAULT NULL COMMENT '配置jobProperties定义的枚举控制Elastic-Job的实现细节', "
                + "`streaming_process` int(1) DEFAULT NULL COMMENT '是否流式处理数据', "
                + "`script_command_line` text COMMENT '脚本型作业执行命令行', "
                + "`monitor_execution` int(4) DEFAULT NULL COMMENT '监控作业运行时状态', "
                + "`monitor_port` int(7) DEFAULT NULL COMMENT '作业监控端口', "
                + "`max_time_diffSeconds` int(11) DEFAULT NULL COMMENT '最大允许的本机与注册中心的时间误差秒数', "
                + "`job_sharding_strategy_class` text COMMENT '作业分片策略实现类全路径', "
                + "`reconcile_interval_minutes` int(11) DEFAULT NULL COMMENT '修复作业服务器不一致状态服务调度间隔时间，配置为小于1的任意值表示不执行修复', "
                + "`event_trace_rdb_dataSource` varchar(255) DEFAULT NULL COMMENT '作业事件追踪的数据源Bean引用', "
                + "`overwrite` int(1) DEFAULT NULL COMMENT '本地配置是否可覆盖注册中心配置.', "
                + "`disabled` int(1) DEFAULT NULL COMMENT '作业是否禁止启动', "
                + "`listener` text COMMENT '每台作业节点均执行的监听', "
                + "`distributed_listener` text COMMENT '分布式场景中仅单一节点执行的监听', "
                + "`started_timeout_milliseconds` bigint(20) DEFAULT NULL COMMENT '最后一个作业执行前的执行方法的超时时间', "
                + "`completed_timeout_milliseconds` bigint(20) DEFAULT NULL, "
                + "`valid` bigint(1) NOT NULL DEFAULT '1' COMMENT '删除标识 1：未删除、2：已删除', "
                + "`status` int(1) DEFAULT NULL COMMENT '状态 1 启用  2禁用', "
                + "`update_time` bigint(10) DEFAULT NULL COMMENT '更新时间 使用linux 下时间戳', "
                + "`updater` bigint(20) DEFAULT NULL COMMENT '更新人', "
                + "``remark` text COMMENT '备注',"
                + "PRIMARY KEY (`id`), "
                + "UNIQUE KEY `job_name_unique` (`job_name`)"
                + ")ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='定时任务表'";
        try (PreparedStatement preparedStatement = conn.prepareStatement(dbSchema)) {
            preparedStatement.execute();
        }
    }
    
    private boolean isDuplicateRecord(final SQLException ex) {
        return DatabaseType.MySQL.equals(databaseType) && 1062 == ex.getErrorCode() || DatabaseType.H2.equals(databaseType) && 23505 == ex.getErrorCode() 
                || DatabaseType.SQLServer.equals(databaseType) && 1 == ex.getErrorCode() || DatabaseType.DB2.equals(databaseType) && -803 == ex.getErrorCode()
                || DatabaseType.PostgreSQL.equals(databaseType) && 0 == ex.getErrorCode() || DatabaseType.Oracle.equals(databaseType) && 1 == ex.getErrorCode();
    }
    
}
