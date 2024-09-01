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
package io.datavines.connector.api;

import io.datavines.common.enums.DataType;
import io.datavines.common.utils.StringUtils;
import io.datavines.connector.api.entity.StructField;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface Dialect {

    String getDriver();

    String getColumnPrefix();

    String getColumnSuffix();

    default Map<String,String> getDialectKeyMap() {
        return new HashMap<>();
    }

    List<String> getExcludeDatabases();

    default String getFullQualifiedTableName(String database, String schema, String table, boolean needQuote) {

        if (needQuote) {
            table = quoteIdentifier(table);

            if (!StringUtils.isEmptyOrNullStr(schema)) {
                table = quoteIdentifier(schema) + "." + table;
            }

            if (!StringUtils.isEmptyOrNullStr(database)) {
                table = quoteIdentifier(database) + "." + table;
            }

        } else {

            if (!StringUtils.isEmptyOrNullStr(schema)) {
                table = schema + "." + table;
            }

            if (!StringUtils.isEmptyOrNullStr(database)) {
                table = database + "." + table;
            }
        }

        return table;
    }

    default boolean invalidateItemCanOutput(){
        return true;
    }

    default boolean invalidateItemCanOutputToSelf(){
        return false;
    }

    default boolean supportToBeErrorDataStorage(){
        return false;
    }

    default String getJDBCType(DataType dataType){
        return dataType.toString();
    }

    default DataType getDataType(String jdbcType) {
        return DataType.valueOf(jdbcType);
    }

    default String quoteIdentifier(String entity) {
        return entity;
    }

    default String getTableExistsQuery(String table) {
        return String.format("SELECT * FROM %s WHERE 1=0", table);
    }

    default String getSchemaQuery(String table) {
        return String.format("SELECT * FROM %s WHERE 1=0", table);
    }

    default String getCountQuery(String table) {
        return String.format("SELECT COUNT(1) FROM %s", table);
    }

    default String getSelectQuery(String table) {
        return String.format("SELECT * FROM %s", table);
    }

    default String getCreateTableAsSelectStatement(String srcTable, String targetDatabase, String targetTable) {
        return String.format("CREATE TABLE %s.%s AS SELECT * FROM %s", quoteIdentifier(targetDatabase), quoteIdentifier(targetTable), quoteIdentifier(srcTable));
    }

    default String getCreateTableAsSelectStatementFromSql(String srcTable, String targetDatabase, String targetTable) {
        return String.format("CREATE TABLE %s.%s AS SELECT t.* FROM %s", quoteIdentifier(targetDatabase), quoteIdentifier(targetTable), srcTable);
    }

    default String getCreateTableStatement(String table, List<StructField> fields, TypeConverter typeConverter) {
        if (CollectionUtils.isNotEmpty(fields)) {
            String columns = fields.stream().map(field -> {
                return quoteIdentifier(field.getName()) + " " + typeConverter.convertToOriginType(field.getDataType());
            }).collect(Collectors.joining(","));

            return String.format("CREATE TABLE IF NOT EXISTS %s (%s)", table, columns);
        }

        return "";
    }

    default String getInsertAsSelectStatement(String srcTable, String targetDatabase, String targetTable) {
        return String.format("INSERT INTO %s.%s SELECT * FROM %s", quoteIdentifier(targetDatabase), quoteIdentifier(targetTable), quoteIdentifier(srcTable));
    }

    default String getInsertAsSelectStatementFromSql(String srcTable, String targetDatabase, String targetTable) {
        return String.format("INSERT INTO %s.%s SELECT t.* FROM %s", quoteIdentifier(targetDatabase), quoteIdentifier(targetTable), srcTable);
    }

    String getErrorDataScript(Map<String, String> configMap);

    String getValidateResultDataScript(Map<String, String> configMap);
}
