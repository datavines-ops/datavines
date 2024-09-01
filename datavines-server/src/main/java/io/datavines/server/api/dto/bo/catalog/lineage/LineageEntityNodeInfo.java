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
package io.datavines.server.api.dto.bo.catalog.lineage;

import io.datavines.server.api.dto.bo.catalog.CatalogEntityInstanceInfo;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class LineageEntityNodeInfo implements Serializable {

    private static final long serialVersionUID = -1L;

    private Long id;

    private String uuid;

    private Long datasourceId;

    private String type;

    private String fullyQualifiedName;

    private String displayName;

    private String description;

    private CatalogEntityInstanceInfo datasource;

    private CatalogEntityInstanceInfo database;

    private CatalogEntityInstanceInfo schema;

    private CatalogEntityInstanceInfo catalog;

    private List<CatalogEntityInstanceInfo> columns;

    private boolean hasNextNode;

}
