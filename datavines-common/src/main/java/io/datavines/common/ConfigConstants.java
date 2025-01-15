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
package io.datavines.common;

public class ConfigConstants {

    public static final String ENGINE_TYPE = "engine_type";
    public static final String CONNECTOR_TYPE = "connector_type";
    public static final String DATASOURCE_ID = "datasource_id";
    public static final String TABLE = "table";
    public static final String VIEW_SQL = "view_sql";
    public static final String TABLE_ALIAS = "table_alias";
    public static final String TABLE2_ALIAS = "table2_alias";
    public static final String TABLE_ALIAS_COLUMNS = "table_alias_columns";
    public static final String TABLE2_ALIAS_COLUMNS = "table2_alias_columns";
    public static final String FILTER = "filter";
    public static final String COLUMN = "column";
    public static final String SRC_CONNECTOR_TYPE = "src_connector_type";
    public static final String TARGET_CONNECTOR_TYPE = "target_connector_type";
    public static final String TARGET_DATASOURCE_ID = "target_datasource_id";
    public static final String TARGET_TABLE = "target_table";
    public static final String TARGET_FILTER = "target_filter";
    public static final String TARGET_COLUMN = "target_column";
    public static final String ACTUAL_NAME = "actual_name";
    public static final String ACTUAL_EXECUTE_SQL = "actual_execute_sql";
    public static final String ACTUAL_AGGREGATE_SQL = "actual_aggregate_sql";
    public static final String ACTUAL_CUSTOM_SQL = "actual_custom_sql";
    public static final String EXPECTED_NAME = "expected_name";
    public static final String EXPECTED_TYPE = "expected_type";
    public static final String EXPECTED_TABLE = "expected_table";
    public static final String EXPECTED_VALUE = "expected_value";
    public static final String EXPECTED_VALUE_DEFAULT = "expected_value_default";
    public static final String EXPECTED_EXECUTE_SQL = "expected_execute_sql";
    public static final String MAPPING_COLUMNS = "mappingColumns";
    public static final String ON_CLAUSE = "on_clause";
    public static final String WHERE_CLAUSE = "where_clause";
    public static final String RESULT_FORMULA = "result_formula";
    public static final String THRESHOLD = "threshold";
    public static final String OPERATOR = "operator";
    public static final String FAILURE_STRATEGY = "failure_strategy";
    public static final String ACTUAL_TABLE = "actual_table";
    public static final String ACTUAL_VALUE = "actual_value";
    public static final String AND = " AND ";
    public static final String WRITER_CONNECTOR_TYPE = "writer_connector_type";
    public static final String WRITER_DATASOURCE_ID = "writer_datasource_id";
    public static final String UNIQUE_CODE = "unique_code";
    public static final String DATA_TIME = "data_time";
    public static final String DATA_DATE = "data_date";
    public static final String REGEXP_PATTERN = "regexp_pattern";
    public static final String ERROR_OUTPUT_PATH = "error_output_path";
    public static final String ERROR_DATA_CONNECTOR_TYPE = "error_data_connector_type";
    public static final String INDEX = "index";
    public static final String PATH = "path";
    public static final String HDFS_FILE = "hdfs_file";
    public static final String BATCH = "batch";
    public static final String METRIC_TYPE = "metric_type";
    public static final String METRIC_NAME = "metric_name";
    public static final String METRIC_DIMENSION = "metric_dimension";
    public static final String CREATE_TIME = "create_time";
    public static final String UPDATE_TIME = "update_time";
    public static final String JOB_EXECUTION_ID = "job_execution_id";
    public static final String ERROR_DATA_DIR = "error_data_dir";
    public static final String ERROR_DATA_FILE_NAME = "error_data_file_name";
    public static final String VALIDATE_RESULT_DATA_DIR = "validate_result_data_dir";
    public static final String INVALIDATE_ITEM_CAN_OUTPUT = "invalidate_item_can_output";

    public static final String ERROR_DATA_STORAGE_ID = "error_data_storage_id";
    public static final String ERROR_DATA_OUTPUT_TO_DATASOURCE_DATABASE = "error_data_output_to_datasource_database";
    public static final String DATASOURCE = "datasource";
    public static final String DATABASE = "database";
    public static final String SID = "sid";
    public static final String DATABASE_NAME = "database_name";
    public static final String TABLE_NAME = "table_name";
    public static final String COLUMN_NAME = "column_name";
    public static final String SCHEMA = "schema";
    public static final String SCHEMA2 = "schema2";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DB_TABLE = "dbtable";
    public static final String URL = "url";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String CATALOG = "catalog";
    public static final String PROPERTIES = "properties";
    public static final String TYPE = "type";
    public static final String DRIVER = "driver";
    public static final String SQL = "sql";
    public static final String SPARK_MONGODB_INPUT_URI = "spark.mongodb.input.uri";
    public static final String SPARK_MONGODB_OUTPUT_URI = "spark.mongodb.output.uri";
    public static final String SPARK_MONGODB_INPUT_COLLECTION = "spark.mongodb.input.collection";
    public static final String SPARK_MONGODB_OUTPUT_COLLECTION = "spark.mongodb.output.collection";

    public static final String INPUT_TABLE = "input_table";
    public static final String OUTPUT_TABLE = "output_table";
    public static final String TMP_TABLE = "tmp_table";
    public static final String COLUMN_SEPARATOR = "column_separator";
    public static final String LINE_SEPARATOR = "line_separator";
    public static final String DATA_DIR = "data_dir";

    public static final String ENABLE_SPARK_HIVE_SUPPORT = "enable_spark_hive_support";

    public static final String FILE = "file";

    public static final String METRIC_DATABASE = "metric_database";

    public static final String METRIC_UNIQUE_KEY = "metric_unique_key";
    public static final String FIX_VALUE = "fix_value";

    public static final String TRUE = "true";
    public static final String FALSE = "false";

    //sql dialect on regex key
    public static final String REGEX_KEY = "regex_key";

    public static final String NOT_REGEX_KEY = "not_regex_key";

    public static final String UNIX_TIMESTAMP = "unix_timestamp";

    public static final String STRING_TYPE = "string_type";

    public static final String IF_FUNCTION_KEY = "if_function_key";

    public static final String IF_CASE_KEY = "if_case_key";

    public static final String LIMIT_TOP_50_KEY = "limit_top_50_key";

    public static final String LIMIT_KEY = "limit_key";

    public static final String LENGTH_KEY = "length_key";

    public static final String STD_DEV_KEY = "std_dev_key";

    public static final String VARIANCE_KEY = "variance_key";

    public static final String INVALIDATE_ITEMS_TABLE = "invalidate_items_table";

    /**
     * date format of yyyy-MM-dd HH:mm:ss
     */
    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    /**
     * date format of yyyy-MM-dd HH:mm:ss
     */
    public static final String YYYY_MM_DD = "yyyy-MM-dd";

    public static final String S001 = "\001";

    public static final String DOUBLE_AT = "@@";

    public static final String PRE_SQL = "pre_sql";

    public static final String POST_SQL = "post_sql";

    public static final String SAVE_MODE = "save_mode";

    public static final String UPSERT = "Upsert";

    public static final String FILE_NAME = "file_name";

    public static final String FLINK = "flink";
}
