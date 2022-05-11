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
package io.datavines.common.entity;

public class ExecuteSql {

    private String sql;

    private String resultTable;

    private boolean isErrorOutput;

    public ExecuteSql() {
    }

    public ExecuteSql(String sql, String resultTable) {
        this.sql = sql;
        this.resultTable = resultTable;
        isErrorOutput = false;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getResultTable() {
        return resultTable;
    }

    public void setResultTable(String resultTable) {
        this.resultTable = resultTable;
    }

    public boolean isErrorOutput() {
        return isErrorOutput;
    }

    public void setErrorOutput(boolean errorOutput) {
        isErrorOutput = errorOutput;
    }
}
