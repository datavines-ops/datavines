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
import io.datavines.common.utils.JSONUtils;
import io.datavines.common.utils.LoggerUtils;
import io.datavines.common.utils.OSUtils;
import io.datavines.engine.executor.core.base.AbstractYarnEngineExecutor;
import io.datavines.engine.executor.core.executor.BaseCommandProcess;
import io.datavines.common.utils.YarnUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
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
            this.processResult = shellCommandProcess.run(command);
            logger.info("process result: {}", JSONUtils.toJsonString(this.processResult));
        } catch (Exception e) {
            logger.error("yarn process failure", e);
            throw e;
        }
    }

    @Override
    protected String buildCommand() {
        List<String> commandParts = new ArrayList<>();
        
        // Get FLINK_HOME from configurations or environment variable
        String flinkHome = configurations.getString("flink.home", System.getenv("FLINK_HOME"));
        if (StringUtils.isEmpty(flinkHome)) {
            // Try to find flink in common locations
            String[] commonPaths = {
                "C:\\Program Files\\flink",
                "C:\\flink",
                "/opt/flink",
                "/usr/local/flink"
            };
            
            for (String path : commonPaths) {
                if (new File(path).exists()) {
                    flinkHome = path;
                    break;
                }
            }
            
            if (StringUtils.isEmpty(flinkHome)) {
                throw new RuntimeException("FLINK_HOME is not set and Flink installation could not be found in common locations");
            }
            logger.info("FLINK_HOME not set, using detected path: {}", flinkHome);
        }

        // Build the flink command
        String flinkCmd = Paths.get(flinkHome, "bin", OSUtils.isWindows() ? "flink.cmd" : "flink").toString();
        if (!new File(flinkCmd).exists()) {
            throw new RuntimeException("Flink command not found at: " + flinkCmd);
        }
        
        // Parse application parameters
        String deployMode = "local"; // Default to local mode
        JsonNode envNode = null;
        try {
            String applicationParameter = jobExecutionRequest.getApplicationParameter();
            if (StringUtils.isNotEmpty(applicationParameter)) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode jsonNode = mapper.readTree(applicationParameter);
                envNode = jsonNode.get("env");
                if (envNode != null && envNode.has("deployMode")) {
                    deployMode = envNode.get("deployMode").asText();
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to parse applicationParameter, using default mode: local", e);
        }
        
        // If applicationParameter doesn't specify deployMode, get from configurations
        if ("local".equals(deployMode)) {
            deployMode = configurations.getString("deployMode", "local");
        }
        
        logger.info("Using deploy mode: {}", deployMode);
        
        // Build command parts
        commandParts.add(flinkCmd);
        
        // Add run command and deployment mode specific args
        switch (deployMode.toLowerCase()) {
            case "yarn-session":
                commandParts.add("run");
                commandParts.add("-m");
                commandParts.add("yarn-session");
                addYarnConfig(commandParts, envNode);
                break;
            case "yarn-per-job":
                commandParts.add("run");
                commandParts.add("-m");
                commandParts.add("yarn-per-job");
                addYarnConfig(commandParts, envNode);
                break;
            case "yarn-application":
                commandParts.add("run-application");
                commandParts.add("-t");
                commandParts.add("yarn-application");
                addYarnConfig(commandParts, envNode);
                break;
            default:
                // Local mode
                commandParts.add("run");
                break;
        }
        
        // Add basic parameters
        int parallelism = configurations.getInt("parallelism", 1);
        commandParts.add("-p");
        commandParts.add(String.valueOf(parallelism));
        
        // Add main class
        String mainClass = configurations.getString("mainClass", "io.datavines.engine.flink.core.FlinkDataVinesBootstrap");
        commandParts.add("-c");
        commandParts.add(mainClass);
        
        // Add main jar
        String mainJar = configurations.getString("mainJar");
        if (StringUtils.isEmpty(mainJar)) {
            mainJar = Paths.get(flinkHome, "lib", "datavines-flink-core.jar").toString();
            if (!new File(mainJar).exists()) {
                throw new RuntimeException("Main jar not found at: " + mainJar);
            }
        }
        commandParts.add(mainJar);
        
        // Add program arguments if any
        String programArgs = configurations.getString("programArgs");
        if (StringUtils.isNotEmpty(programArgs)) {
            commandParts.add(programArgs);
        }
        
        return String.join(" ", commandParts);
    }

    private void addYarnConfig(List<String> commandParts, JsonNode envNode) {
        commandParts.add("-Dyarn.application.name=" + jobExecutionRequest.getJobExecutionName());
        
        // Add memory configuration
        String jobManagerMemory = "1024m";
        String taskManagerMemory = "1024m";
        
        if (envNode != null) {
            if (envNode.has("jobmanager.memory.process.size")) {
                jobManagerMemory = envNode.get("jobmanager.memory.process.size").asText("1024m");
            }
            if (envNode.has("taskmanager.memory.process.size")) {
                taskManagerMemory = envNode.get("taskmanager.memory.process.size").asText("1024m");
            }
        }
        
        commandParts.add("-Djobmanager.memory.process.size=" + jobManagerMemory);
        commandParts.add("-Dtaskmanager.memory.process.size=" + taskManagerMemory);
    }

    @Override
    public void after() {
        try {
            if (shellCommandProcess != null) {
                ((FlinkCommandProcess)shellCommandProcess).cleanupTempFiles();
            }
        } catch (Exception e) {
            logger.error("Error in after execution", e);
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
                String cmd = String.format("sudo -u %s yarn application -kill %s", 
                                         jobExecutionRequest.getTenantCode(), 
                                         applicationId);

                logger.info("Killing yarn application: {}", applicationId);
                Runtime.getRuntime().exec(cmd);
            }
        } catch (Exception e) {
            logger.error("Failed to kill yarn application", e);
        }
    }

    @Override
    public void logHandle(List<String> logs) {
        if (logs != null && !logs.isEmpty()) {
            for (String log : logs) {
                logger.info(log);
            }
        }
    }

    @Override
    public boolean isCancel() {
        return this.cancel;
    }
}
