/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.engine.flink.executor;

import org.slf4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datavines.common.config.Configurations;
import io.datavines.common.entity.JobExecutionRequest;
import io.datavines.common.entity.ProcessResult;
import io.datavines.common.enums.ExecutionStatus;
import io.datavines.common.utils.LoggerUtils;
import io.datavines.engine.executor.core.base.AbstractYarnEngineExecutor;
import io.datavines.engine.executor.core.executor.BaseCommandProcess;
import io.datavines.engine.flink.executor.utils.FlinkParameters;
import io.datavines.common.utils.YarnUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;

public class FlinkEngineExecutor extends AbstractYarnEngineExecutor {

    private static final String FLINK_COMMAND = "flink";
    private Configurations configurations;
    private JobExecutionRequest jobExecutionRequest;
    private Logger logger;
    private ProcessResult processResult;
    private BaseCommandProcess shellCommandProcess;
    private boolean cancel;

    @Override
    public void init(JobExecutionRequest jobExecutionRequest, Logger logger, Configurations configurations) throws Exception {
        String threadLoggerInfoName = String.format(LoggerUtils.JOB_LOG_INFO_FORMAT, jobExecutionRequest.getJobExecutionUniqueId());
        Thread.currentThread().setName(threadLoggerInfoName);

        this.jobExecutionRequest = jobExecutionRequest;
        this.logger = logger;
        this.configurations = configurations;
        this.processResult = new ProcessResult();
        this.shellCommandProcess = new FlinkCommandProcess(
            this::logHandle,
            logger,
            jobExecutionRequest,
            configurations
        );
    }

    @Override
    public void execute() throws Exception {
        try {
            String command = buildCommand();
            logger.info("flink task command: {}", command);
            ProcessResult result = shellCommandProcess.run(command);
            
            // Check exit code and set execution result
            if (result.getExitStatusCode() == ExecutionStatus.SUCCESS.getCode()) {
                processResult.setExitStatusCode(ExecutionStatus.SUCCESS.getCode());
                processResult.setProcessId(Integer.valueOf(String.valueOf(jobExecutionRequest.getJobExecutionId())));
                logger.info("Flink job executed successfully");
            } else {
                processResult.setExitStatusCode(ExecutionStatus.FAILURE.getCode());
                processResult.setProcessId(Integer.valueOf(String.valueOf(jobExecutionRequest.getJobExecutionId())));
                String errorMsg = String.format("Flink job execution failed with exit code: %d", result.getExitStatusCode());
                logger.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        } catch (Exception e) {
            logger.error("flink task error", e);
            processResult.setExitStatusCode(ExecutionStatus.FAILURE.getCode());
            processResult.setProcessId(Integer.valueOf(String.valueOf(jobExecutionRequest.getJobExecutionId())));
            throw e;
        }
    }

    @Override
    public void after() throws Exception {
        try {
            if (shellCommandProcess != null) {
                ((FlinkCommandProcess)shellCommandProcess).cleanupTempFiles();
            }
        } catch (Exception e) {
            logger.error("Error in after execution", e);
            // 不抛出异常，避免影响主流程
        }
    }

    @Override
    public ProcessResult getProcessResult() {
        return this.processResult;
    }

    @Override
    public JobExecutionRequest getTaskRequest() {
        return this.jobExecutionRequest;
    }

    @Override
    protected String buildCommand() {
        FlinkParameters parameters = new FlinkParameters();
        
        // 从applicationParameter中获取部署模式
        String deployMode = null;
        JsonNode envNode = null;
        try {
            String applicationParameter = jobExecutionRequest.getApplicationParameter();
            if (applicationParameter != null) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(applicationParameter);
                envNode = jsonNode.get("env");
                if (envNode != null && envNode.has("deployMode")) {
                    deployMode = envNode.get("deployMode").asText();
                }
            }
        } catch (Exception e) {
            logger.error("Failed to parse applicationParameter", e);
        }
        
        // 如果applicationParameter中没有deployMode，则从configurations中获取
        if (deployMode == null) {
            deployMode = configurations.getString("deployMode", "local");  // 默认使用local模式
        }
        
        logger.info("Using deploy mode: {}", deployMode);
        parameters.setDeployMode(deployMode);
        
        // 获取 FLINK_HOME
        String flinkHome = System.getenv("FLINK_HOME");
        if (flinkHome == null || flinkHome.trim().isEmpty()) {
            // 从配置中获取
            flinkHome = configurations.getString("flink.home");
            if (flinkHome == null || flinkHome.trim().isEmpty()) {
                // 使用默认路径
                flinkHome = "/opt/flink";
                logger.info("FLINK_HOME not set, using default path: {}", flinkHome);
            }
        }
        
        // 检查Flink目录是否存在
        File flinkDir = new File(flinkHome);
        if (!flinkDir.exists() || !flinkDir.isDirectory()) {
            logger.warn("Flink directory not found at: {}. Please make sure Flink is properly installed.", flinkHome);
        }
        
        // 构建完整的 Flink 命令路径
        StringBuilder command = new StringBuilder();
        command.append(flinkHome);
        command.append("/bin/").append(FLINK_COMMAND);

        // 根据部署模式添加不同的参数
        switch (deployMode.toLowerCase()) {
            case "yarn-session":
                command.append(" run");
                command.append(" -t yarn-session"); // 指定运行模式为 yarn-session
                command.append(" -Dyarn.application.name=").append(jobExecutionRequest.getJobExecutionName());
                // 添加yarn-session特定的内存配置
                if (envNode != null) {
                    String jobManagerMemory = envNode.get("jobmanager.memory.process.size").asText("1024m");
                    String taskManagerMemory = envNode.get("taskmanager.memory.process.size").asText("1024m");
                    command.append(" -Djobmanager.memory.process.size=").append(jobManagerMemory);
                    command.append(" -Dtaskmanager.memory.process.size=").append(taskManagerMemory);
                }
                break;
            case "yarn-per-job":
                command.append(" run");
                command.append(" -t yarn-per-job"); // 指定运行模式为 yarn-per-job
                command.append(" -Dyarn.application.name=").append(jobExecutionRequest.getJobExecutionName());
                // 添加yarn-per-job特定的内存配置
                if (envNode != null) {
                    String jobManagerMemory = envNode.get("jobmanager.memory.process.size").asText("1024m");
                    String taskManagerMemory = envNode.get("taskmanager.memory.process.size").asText("1024m");
                    command.append(" -Djobmanager.memory.process.size=").append(jobManagerMemory);
                    command.append(" -Dtaskmanager.memory.process.size=").append(taskManagerMemory);
                }
                break;
            case "yarn-application":
                command.append(" run-application");
                command.append(" -t yarn-application"); // 指定运行模式为 yarn-application
                command.append(" -Dyarn.application.name=").append(jobExecutionRequest.getJobExecutionName());
                // 添加yarn-application特定的内存配置
                if (envNode != null) {
                    String jobManagerMemory = envNode.get("jobmanager.memory.process.size").asText("1024m");
                    String taskManagerMemory = envNode.get("taskmanager.memory.process.size").asText("1024m");
                    command.append(" -Djobmanager.memory.process.size=").append(jobManagerMemory);
                    command.append(" -Dtaskmanager.memory.process.size=").append(taskManagerMemory);
                }
                break;
            case "local":
            default:
                command.append(" run");
                // 本地模式不需要添加额外的部署模式参数
                break;
        }

        // 添加通用参数
        command.append(" -p 1");  // 设置并行度为1
        command.append(" -c ").append(configurations.getString("mainClass", "io.datavines.engine.flink.core.FlinkDataVinesBootstrap"));
        
        // 添加主jar包
        String mainJar = configurations.getString("mainJar", flinkHome + "/lib/datavines-flink-core.jar");
        command.append(" ").append(mainJar);

        return command.toString();
    }

    public String getApplicationId() {
        return processResult != null ? processResult.getApplicationId() : null;
    }

    public String getApplicationUrl() {
        return processResult != null ? YarnUtils.getApplicationUrl(processResult.getApplicationId()) : null;
    }

    @Override
    public void cancel() throws Exception {
        cancel = true;
        if (shellCommandProcess != null) {
            shellCommandProcess.cancel();
        }
        killYarnApplication();
    }

    private void killYarnApplication() {
        try {
            String applicationId = YarnUtils.getYarnAppId(jobExecutionRequest.getTenantCode(), 
                                                        jobExecutionRequest.getJobExecutionUniqueId());

            if (StringUtils.isNotEmpty(applicationId)) {
                // sudo -u user command to run command
                String cmd = String.format("sudo -u %s yarn application -kill %s", 
                                         jobExecutionRequest.getTenantCode(), 
                                         applicationId);

                logger.info("yarn application -kill {}", applicationId);
                Runtime.getRuntime().exec(cmd);
            }
        } catch (Exception e) {
            logger.error("kill yarn application failed", e);
        }
    }

    @Override
    public void logHandle(List<String> logs) {
        // 处理日志输出
        if (logs != null && !logs.isEmpty()) {
            for (String log : logs) {
                logger.info(log);
                // 可以在这里添加对日志的解析，比如提取applicationId等信息
                if (log.contains("Job has been submitted with JobID")) {
                    String jobId = extractJobId(log);
                    if (jobId != null) {
                        processResult.setProcessId(Integer.valueOf(jobId));
                    }
                }
                // 尝试从日志中提取Yarn Application ID
                if (log.contains("Submitted application")) {
                    String appId = extractYarnAppId(log);
                    if (appId != null) {
                        processResult.setApplicationId(appId);
                    }
                }
            }
        }
    }

    private String extractJobId(String log) {
        // 从日志中提取JobID的简单实现
        int index = log.indexOf("JobID");
        if (index != -1) {
            return log.substring(index).trim();
        }
        return null;
    }

    private String extractYarnAppId(String log) {
        // 从日志中提取Yarn Application ID的简单实现
        int index = log.indexOf("application_");
        if (index != -1) {
            String appId = log.substring(index);
            // 提取到第一个空格为止
            int spaceIndex = appId.indexOf(" ");
            if (spaceIndex != -1) {
                appId = appId.substring(0, spaceIndex);
            }
            return appId;
        }
        return null;
    }

    @Override
    public boolean isCancel() {
        return cancel;
    }
}
