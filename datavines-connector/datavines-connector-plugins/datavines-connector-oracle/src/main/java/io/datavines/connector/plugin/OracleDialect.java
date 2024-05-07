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
package io.datavines.connector.plugin;

import io.datavines.common.utils.StringUtils;

import java.util.Map;

import static io.datavines.common.ConfigConstants.*;

public class OracleDialect extends JdbcDialect{
    @Override
    public Map<String, String> getDialectKeyMap() {
        super.getDialectKeyMap();
        dialectKeyMap.put(STRING_TYPE, "VARCHAR2");
        dialectKeyMap.put(LIMIT_TOP_50_KEY, " rownum <= 50");
        dialectKeyMap.put(REGEX_KEY, "regexp_like(${column},'${regexp}')");
        dialectKeyMap.put(NOT_REGEX_KEY, "not regexp_like(${column},'${regexp}')");
        dialectKeyMap.put(IF_CASE_KEY, "case when ${column} is null then 'NULL' else ${column}||'' end ");
        return dialectKeyMap;
    }

    @Override
    public String getDriver() {
        return "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    public boolean invalidateItemCanOutputToSelf() {
        return true;
    }

    @Override
    public boolean supportToBeErrorDataStorage() {
        return false;
    }

    @Override
    public String getCreateTableAsSelectStatement(String srcTable, String targetDatabase, String targetTable) {
        return String.format("CREATE TABLE %s.%s AS SELECT * FROM %s", quoteIdentifier(targetDatabase), quoteIdentifier(targetTable), quoteIdentifier(srcTable));
    }

    @Override
    public String quoteIdentifier(String column) {
        return column;
    }

    @Override
    public String getFullQualifiedTableName(String database, String schema, String table,boolean needQuote) {
        table = quoteIdentifier(table);

        if (!StringUtils.isEmptyOrNullStr(schema)) {
            table = quoteIdentifier(schema) + "." + table;
        }

        return table;
    }
}
