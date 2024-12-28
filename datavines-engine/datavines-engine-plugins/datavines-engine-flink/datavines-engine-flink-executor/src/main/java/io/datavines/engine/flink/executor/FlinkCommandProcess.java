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
import io.datavines.common.utils.ProcessUtils;
import io.datavines.engine.executor.core.executor.BaseCommandProcess;
import org.slf4j.Logger;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class FlinkCommandProcess extends BaseCommandProcess {

    private static final String SH = "sh";

    public FlinkCommandProcess(Consumer<List<String>> logHandler,
                             Logger logger,
                             JobExecutionRequest jobExecutionRequest,
                             Configurations configurations) {
        super(logHandler, logger, jobExecutionRequest, configurations);
    }

    @Override
    protected String buildCommandFilePath() {
        return String.format("%s/%s.command", jobExecutionRequest.getExecuteFilePath(), jobExecutionRequest.getJobExecutionId());
    }

    @Override
    protected void createCommandFileIfNotExists(String execCommand, String commandFile) throws IOException {
        logger.info("tenant {},job dir:{}", jobExecutionRequest.getTenantCode(), jobExecutionRequest.getExecuteFilePath());

        Path commandFilePath = Paths.get(commandFile);
        // 确保父目录存在
        Files.createDirectories(commandFilePath.getParent());
        
        if(Files.exists(commandFilePath)){
            Files.delete(commandFilePath);
        }

        logger.info("create command file:{}",commandFile);

        StringBuilder sb = new StringBuilder();
        sb.append("#!/bin/sh\n");
        sb.append("BASEDIR=$(cd `dirname $0`; pwd)\n");
        sb.append("cd $BASEDIR\n");
        sb.append("\n");
        sb.append(execCommand);

        // 设置文件权限为可执行
        Files.write(commandFilePath, sb.toString().getBytes());
        commandFilePath.toFile().setExecutable(true, false);
    }

    @Override
    protected String commandInterpreter() {
        return SH;
    }

    @Override
    protected List<String> commandOptions() {
        List<String> options = new LinkedList<>();
        options.add("-c");
        return options;
    }

    public void cleanupTempFiles() {
        try {
            String commandFile = buildCommandFilePath();
            Path commandPath = Paths.get(commandFile);
            if (Files.exists(commandPath)) {
                Files.delete(commandPath);
                logger.info("Cleaned up command file: {}", commandFile);
            }
        } catch (IOException e) {
            logger.warn("Failed to cleanup command file", e);
        }
    }

    private void buildProcess(String commandFile) throws IOException {
        // Create process builder
        ProcessBuilder processBuilder = buildProcessBuilder(commandFile);
        // merge error information to standard output stream
        processBuilder.redirectErrorStream(true);

        // Print command for debugging
        try {
            String cmdStr = ProcessUtils.buildCommandStr(processBuilder.command());
            logger.info("job run command:\n{}", cmdStr);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        // Start the process
        try {
            Process tempProcess = processBuilder.start();
            // Use reflection to set the process field in parent class
            Field processField = BaseCommandProcess.class.getDeclaredField("process");
            processField.setAccessible(true);
            processField.set(this, tempProcess);
        } catch (Exception e) {
            logger.error("Failed to start or set process: " + e.getMessage(), e);
            throw new IOException("Failed to start process", e);
        }
    }

    private ProcessBuilder buildProcessBuilder(String commandFile) {
        List<String> commandList = new ArrayList<>();
        
        // 直接执行命令，不使用sudo
        commandList.add(commandInterpreter());
        commandList.addAll(commandOptions());
        commandList.add(commandFile);

        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        processBuilder.directory(new File(jobExecutionRequest.getExecuteFilePath()));
        
        // 添加环境变量
        Map<String, String> env = processBuilder.environment();
        env.put("FLINK_HOME", System.getenv("FLINK_HOME"));
        
        return processBuilder;
    }
}
