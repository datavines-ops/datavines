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
package io.datavines.server.repository.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.service.IService;
import io.datavines.server.api.dto.bo.catalog.CatalogRefresh;
import io.datavines.server.api.dto.vo.catalog.CatalogMetaDataFetchTaskVO;
import io.datavines.server.repository.entity.CommonTask;

import java.time.LocalDateTime;
import java.util.List;

public interface CommonTaskService extends IService<CommonTask> {

    long refreshCatalog(CatalogRefresh catalogRefresh);

    int update(CommonTask commonTask);

    CommonTask getById(long id);

    Long killCatalogTask(Long catalogTaskId);

    List<CommonTask> listNeedFailover(String host);

    List<CommonTask> listTaskNotInServerList(List<String> hostList);

    String getTaskExecuteHost(Long catalogTaskId);

    boolean deleteByDataSourceId(long dataSourceId);

    LocalDateTime getRefreshTime(long dataSourceId, String databaseName, String tableName);

    IPage<CatalogMetaDataFetchTaskVO> getFetchTaskPage(Long datasourceId, String taskType, Integer pageNumber, Integer pageSize);
}
