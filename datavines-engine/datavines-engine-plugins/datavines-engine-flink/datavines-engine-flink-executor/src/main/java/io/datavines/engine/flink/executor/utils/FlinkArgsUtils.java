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
package io.datavines.engine.flink.executor.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class FlinkArgsUtils {

    public static List<String> buildArgs(FlinkParameters parameters) {
        List<String> args = new ArrayList<>();

        // Add run command
        args.add("run");

        // Add deploy mode
        if ("cluster".equalsIgnoreCase(parameters.getDeployMode())) {
            args.add("-m");
            args.add("yarn-cluster");
        }

        // Add parallelism
        if (parameters.getParallelism() > 0) {
            args.add("-p");
            args.add(String.valueOf(parameters.getParallelism()));
        }

        // Add job name
        if (StringUtils.isNotEmpty(parameters.getJobName())) {
            args.add("-Dyarn.application.name=" + parameters.getJobName());
        }

        // Add yarn queue
        if (StringUtils.isNotEmpty(parameters.getYarnQueue())) {
            args.add("-Dyarn.application.queue=" + parameters.getYarnQueue());
        }

        // Add main jar
        if (StringUtils.isNotEmpty(parameters.getMainJar())) {
            args.add(parameters.getMainJar());
        }

        // Add main class
        if (StringUtils.isNotEmpty(parameters.getMainClass())) {
            args.add(parameters.getMainClass());
        }

        // Add program arguments
        if (StringUtils.isNotEmpty(parameters.getMainArgs())) {
            args.add(parameters.getMainArgs());
        }

        return args;
    }
}
