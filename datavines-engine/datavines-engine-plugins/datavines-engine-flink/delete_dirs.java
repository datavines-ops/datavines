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
import java.io.File;

public class DeleteDirs {
    public static void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        dir.delete();
    }

    public static void main(String[] args) {
        String basePath = "e:/datavines-new/datavines-engine/datavines-engine-plugins/datavines-engine-flink/datavines-engine-flink-core/src/main/java/io/datavines/engine/flink/";
        String[] dirs = {"config", "sink", "source", "transform"};
        
        for (String dir : dirs) {
            File file = new File(basePath + dir);
            if (file.exists()) {
                deleteDirectory(file);
                System.out.println("Deleted directory: " + file.getAbsolutePath());
            }
        }
    }
}
