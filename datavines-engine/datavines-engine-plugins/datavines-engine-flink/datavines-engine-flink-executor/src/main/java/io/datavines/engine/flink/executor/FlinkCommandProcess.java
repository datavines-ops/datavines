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

import io.datavines.common.config.Configurations;
import io.datavines.common.entity.JobExecutionRequest;
import io.datavines.common.entity.ProcessResult;
import io.datavines.common.enums.ExecutionStatus;
import io.datavines.common.utils.OSUtils;
import io.datavines.common.utils.ProcessUtils;
import io.datavines.common.utils.YarnUtils;
import io.datavines.engine.executor.core.executor.BaseCommandProcess;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class FlinkCommandProcess extends BaseCommandProcess {

    private static final String FLINK_COMMAND = "flink";
    private long timeout;

    public FlinkCommandProcess(Consumer<List<String>> logHandler,
                             Logger logger,
                             JobExecutionRequest jobExecutionRequest,
                             Configurations configurations) {
        super(logHandler, logger, jobExecutionRequest, configurations);
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private ProcessBuilder buildFlinkProcessBuilder(String command) throws IOException {
        List<String> commandList = new ArrayList<>();
        
        // Get flink home from configurations or environment variable
        String flinkHome = configurations.getString("flink.home", System.getenv("FLINK_HOME"));
        if (StringUtils.isEmpty(flinkHome)) {
            throw new IOException("FLINK_HOME is not set in either configurations or environment variables");
        }

        // Build the flink command path
        String flinkCmd = Paths.get(flinkHome, "bin", OSUtils.isWindows() ? "flink.cmd" : "flink").toString();
        
        // Split the command while preserving quoted spaces
        String[] cmdArray = command.split("\\s+(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
        commandList.add(flinkCmd);
        for (String cmd : cmdArray) {
            // Remove quotes
            cmd = cmd.replaceAll("^\"|\"$", "");
            if (!cmd.trim().isEmpty()) {
                commandList.add(cmd);
            }
        }

        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        processBuilder.directory(new File(jobExecutionRequest.getExecuteFilePath()));
        
        // Set environment variables
        Map<String, String> env = processBuilder.environment();
        env.put("FLINK_HOME", flinkHome);
        
        // Add to PATH
        String path = env.get("PATH");
        if (path != null) {
            path = Paths.get(flinkHome, "bin") + File.pathSeparator + path;
            env.put("PATH", path);
        }
        
        return processBuilder;
    }

    @Override
    public ProcessResult run(String executeCommand) {
        if (StringUtils.isEmpty(executeCommand)) {
            ProcessResult result = new ProcessResult();
            result.setExitStatusCode(ExecutionStatus.FAILURE.getCode());
            return result;
        }

        try {
            ProcessBuilder processBuilder = buildFlinkProcessBuilder(executeCommand);
            processBuilder.redirectErrorStream(true);
            
            // 打印命令用于调试
            logger.info("Executing command: {}", String.join(" ", processBuilder.command()));
            
            Process process = processBuilder.start();
            ProcessResult result = new ProcessResult();
            
            try {
                // 启动日志处理线程
                startLogHandler(process);
                
                if (timeout > 0) {
                    boolean completed = process.waitFor(timeout, TimeUnit.MILLISECONDS);
                    if (!completed) {
                        process.destroyForcibly();
                        throw new IOException("Process timed out after " + timeout + "ms");
                    }
                } else {
                    process.waitFor();
                }
                
                int exitCode = process.exitValue();
                result.setExitStatusCode(exitCode);
                
                // 获取Yarn应用ID（如果有）
                String appId = YarnUtils.getYarnAppId(jobExecutionRequest.getTenantCode(), 
                                                    jobExecutionRequest.getJobExecutionUniqueId());
                if (StringUtils.isNotEmpty(appId)) {
                    result.setApplicationId(appId);
                    // 根据Yarn状态确定最终状态
                    if (exitCode == 0) {
                        result.setExitStatusCode(YarnUtils.isSuccessOfYarnState(appId) ? 
                            ExecutionStatus.SUCCESS.getCode() : ExecutionStatus.FAILURE.getCode());
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Process was interrupted", e);
            }
            
            return result;
        } catch (IOException e) {
            throw new RuntimeException("Error executing process", e);
        }
    }

    private void startLogHandler(Process process) {
        if (getLogHandler() != null) {
            Thread logThread = new Thread(() -> {
                try {
                    ProcessUtils.printProcessOutput(process, getLogHandler());
                } catch (IOException e) {
                    logger.error("Error handling process output", e);
                }
            });
            logThread.setDaemon(true);
            logThread.start();
        }
    }

    @Override
    protected String commandInterpreter() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'commandInterpreter'");
    }

    @Override
    protected String buildCommandFilePath() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'buildCommandFilePath'");
    }

    @Override
    protected void createCommandFileIfNotExists(String execCommand, String commandFile) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'createCommandFileIfNotExists'");
    }

    /**
     * Cleanup any temporary files created during the Flink job execution
     */
    public void cleanupTempFiles() {
        String commandFile = buildCommandFilePath();
        if (commandFile != null) {
            File file = new File(commandFile);
            if (file.exists() && file.isFile()) {
                if (!file.delete()) {
                    logger.warn("Failed to delete temporary command file: {}", commandFile);
                }
            }
        }
    }
}
