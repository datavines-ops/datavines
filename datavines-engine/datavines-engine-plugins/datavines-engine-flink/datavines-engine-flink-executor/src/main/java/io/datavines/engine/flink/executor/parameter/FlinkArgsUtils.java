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
package io.datavines.engine.flink.executor.parameter;

import java.util.ArrayList;
import java.util.List;

public class FlinkArgsUtils {

    private static final String FLINK_LOCAL = "local";
    private static final String FLINK_YARN_SESSION = "yarn-session";
    private static final String FLINK_YARN_PER_JOB = "yarn-per-job";
    private static final String FLINK_YARN_APPLICATION = "yarn-application";

    private FlinkArgsUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static List<String> buildArgs(FlinkParameters param) {
        List<String> args = new ArrayList<>();

        // Add run command based on deployment mode
        String deployMode = param.getDeployMode();
        if (deployMode == null || deployMode.isEmpty()) {
            deployMode = FLINK_LOCAL; // Default to local mode
        }

        switch (deployMode.toLowerCase()) {
            case "yarn-session":
                args.add("run");
                args.add("-m");
                args.add("yarn-session");
                break;
            case "yarn-per-job":
                args.add("run");
                args.add("-m");
                args.add("yarn-per-job");
                break;
            case "yarn-application":
                args.add("run-application");
                args.add("-t");
                args.add("yarn-application");
                break;
            case "local":
            default:
                args.add("run");
                break;
        }

        // Add parallelism
        if (param.getParallelism() > 0) {
            args.add("-p");
            args.add(String.valueOf(param.getParallelism()));
        }

        // Add job name if specified (only for YARN modes)
        if (!FLINK_LOCAL.equals(deployMode) && param.getJobName() != null && !param.getJobName().isEmpty()) {
            args.add("-Dyarn.application.name=" + param.getJobName());
        }

        // Add yarn queue if specified (only for YARN modes)
        if (!FLINK_LOCAL.equals(deployMode) && param.getYarnQueue() != null && !param.getYarnQueue().isEmpty()) {
            args.add("-Dyarn.application.queue=" + param.getYarnQueue());
        }

        // Add main class
        if (param.getMainClass() != null && !param.getMainClass().isEmpty()) {
            args.add("-c");
            args.add(param.getMainClass());
        }

        // Add jar file
        args.add(param.getMainJar());

        // Add program arguments if any
        if (param.getMainArgs() != null && !param.getMainArgs().isEmpty()) {
            args.add(param.getMainArgs());
        }

        return args;
    }
}
