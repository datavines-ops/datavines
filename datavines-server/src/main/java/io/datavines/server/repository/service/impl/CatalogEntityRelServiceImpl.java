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
package io.datavines.server.repository.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import io.datavines.common.datasource.jdbc.JdbcConnectionInfo;
import io.datavines.common.enums.EntityRelType;
import io.datavines.common.utils.JSONUtils;
import io.datavines.common.utils.StringUtils;
import io.datavines.connector.api.ConnectorFactory;
import io.datavines.connector.api.LineageParser;
import io.datavines.connector.api.entity.ScriptMetadata;
import io.datavines.connector.api.entity.StatementMetadata;
import io.datavines.connector.api.entity.StatementMetadataFragment;
import io.datavines.core.enums.Status;
import io.datavines.core.exception.DataVinesServerException;
import io.datavines.server.api.dto.bo.catalog.CatalogEntityInstanceInfo;
import io.datavines.server.api.dto.bo.catalog.lineage.*;
import io.datavines.server.api.dto.bo.datasource.DataSourceInfo;
import io.datavines.server.api.dto.vo.catalog.lineage.CatalogEntityLineageVO;
import io.datavines.server.enums.LineageSourceType;
import io.datavines.server.repository.entity.DataSource;
import io.datavines.server.repository.entity.catalog.CatalogEntityInstance;
import io.datavines.server.repository.entity.catalog.CatalogEntityRel;
import io.datavines.server.repository.mapper.CatalogEntityRelMapper;
import io.datavines.server.repository.service.CatalogEntityInstanceService;
import io.datavines.server.repository.service.CatalogEntityRelService;
import io.datavines.server.repository.service.DataSourceService;
import io.datavines.server.utils.ContextHolder;
import io.datavines.spi.PluginLoader;
import org.apache.commons.collections4.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service("catalogEntityRelService")
public class CatalogEntityRelServiceImpl extends ServiceImpl<CatalogEntityRelMapper, CatalogEntityRel> implements CatalogEntityRelService {

    @Autowired
    private CatalogEntityInstanceService catalogEntityInstanceService;

    @Autowired
    private DataSourceService dataSourceService;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public boolean addLineage(LineageEntityEdgeInfo entityEdgeInfo) {
        String fromUUID = entityEdgeInfo.getFromEntity().getUuid();
        String toUUID = entityEdgeInfo.getToEntity().getUuid();
        CatalogEntityRel rel = getOne(new LambdaQueryWrapper<CatalogEntityRel>()
                .eq(CatalogEntityRel::getEntity1Uuid, fromUUID)
                .eq(CatalogEntityRel::getEntity2Uuid, toUUID)
                .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()), false);
        if (rel == null) {
            rel = new CatalogEntityRel();
        }

        rel.setEntity1Uuid(fromUUID);
        rel.setEntity2Uuid(toUUID);
        rel.setType(EntityRelType.DOWNSTREAM.getDescription());
        rel.setSourceType(entityEdgeInfo.getLineageDetail().getSourceType().getDescription());
        rel.setRelatedScript(entityEdgeInfo.getLineageDetail().getSqlQuery());
        rel.setUpdateBy(ContextHolder.getUserId());
        rel.setUpdateTime(LocalDateTime.now());

        boolean result;
        if (rel.getId() == null) {
            result = save(rel);
        } else {
            result = updateById(rel);
        }

        if (result) {
            CatalogEntityLineageDetail catalogEntityLineageDetail = entityEdgeInfo.getLineageDetail();
            List<CatalogEntityColumnLineageDetail> detailList = catalogEntityLineageDetail.getChildRelDetailList();
            if (CollectionUtils.isEmpty(detailList)) {
                return true;
            }

            for (CatalogEntityColumnLineageDetail detail : detailList) {
                List<CatalogEntityInstanceInfo> fromEntityList = detail.getFromChildren();
                if (CollectionUtils.isEmpty(fromEntityList)) {
                    continue;
                }

                CatalogEntityInstanceInfo toEntity = detail.getToChild();
                if (toEntity == null) {
                    continue;
                }

                for (CatalogEntityInstanceInfo fromEntity : fromEntityList) {
                    CatalogEntityRel columnRel = getOne(new LambdaQueryWrapper<CatalogEntityRel>()
                            .eq(CatalogEntityRel::getEntity1Uuid, fromEntity.getUuid())
                            .eq(CatalogEntityRel::getEntity2Uuid, toEntity.getUuid())
                            .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()), false);
                    if (columnRel == null) {
                        columnRel = new CatalogEntityRel();
                    }

                    columnRel.setEntity1Uuid(fromEntity.getUuid());
                    columnRel.setEntity2Uuid(toEntity.getUuid());
                    columnRel.setType(EntityRelType.DOWNSTREAM.getDescription());
                    columnRel.setSourceType(catalogEntityLineageDetail.getSourceType().getDescription());
                    columnRel.setRelatedScript(catalogEntityLineageDetail.getSqlQuery());
                    columnRel.setUpdateBy(ContextHolder.getUserId());
                    columnRel.setUpdateTime(LocalDateTime.now());

                    if (columnRel.getId() == null) {
                        save(columnRel);
                    } else {
                        updateById(columnRel);
                    }
                }
            }

            return true;
        }

        return false;
    }

    @Override
    public boolean addLineageByParseSql(SqlWithDataSourceList sqlWithDataSourceList) {
        if (sqlWithDataSourceList == null) {
            return false;
        }

        List<DataSourceInfo> dataSourceInfos = sqlWithDataSourceList.getDataSourceInfos();
        if (CollectionUtils.isEmpty(dataSourceInfos)) {
            return false;
        }

        String sql = sqlWithDataSourceList.getSql();

        for (DataSourceInfo dataSourceInfo: dataSourceInfos) {
            ConnectorFactory connectorFactory = PluginLoader.getPluginLoader(ConnectorFactory.class).getOrCreatePlugin(dataSourceInfo.getType());
            if (connectorFactory == null) {
                continue;
            }

            DataSource dataSource = dataSourceService.getDataSourceById(dataSourceInfo.getId());

            JdbcConnectionInfo jdbcConnectionInfo = JSONUtils.parseObject(dataSource.getParam(), JdbcConnectionInfo.class);

            ScriptMetadata scriptMetadata = LineageParser.parseScript(sql, connectorFactory.getStatementSplitter(), connectorFactory.getStatementParser());
            if (scriptMetadata == null) {
                continue;
            }

            List<StatementMetadata> statementMetadataList = scriptMetadata.getStatementMetadataList();
            if (CollectionUtils.isEmpty(statementMetadataList)) {
                continue;
            }

            for (StatementMetadata statementMetadata: statementMetadataList) {
                StatementMetadataFragment statementMetadataFragment = statementMetadata.getStatementMetadataFragment();
                if (statementMetadataFragment == null) {
                    continue;
                }

                List<String> inputTables = statementMetadataFragment.getInputTables();
                List<String> outputTables = statementMetadataFragment.getOutputTables();
                if (CollectionUtils.isEmpty(inputTables) || CollectionUtils.isEmpty(outputTables)) {
                    continue;
                }

                for (String inputTable : inputTables) {
                    for (String outputTable : outputTables) {
                        if (StringUtils.isEmpty(inputTable) || StringUtils.isEmpty(outputTable)) {
                            continue;
                        }

                        if (jdbcConnectionInfo!= null && !inputTable.contains(".")) {
                            inputTable = jdbcConnectionInfo.getDatabase() + "." + inputTable;
                        }

                        if (jdbcConnectionInfo!= null && !outputTable.contains(".")) {
                            outputTable = jdbcConnectionInfo.getDatabase() + "." + outputTable;
                        }

                        CatalogEntityInstance fromEntity = catalogEntityInstanceService.getByDataSourceAndFQN(dataSourceInfo.getId(), inputTable);
                        CatalogEntityInstance toEntity = catalogEntityInstanceService.getByDataSourceAndFQN(dataSourceInfo.getId(), outputTable);
                        if (fromEntity == null || toEntity == null) {
                            continue;
                        }

                        return addLineage(fromEntity.getUuid(), toEntity.getUuid(), LineageSourceType.SQL_PARSER, statementMetadata.getStatementText());
                    }
                }
            }
        }

        return false;
    }

    @Override
    public boolean addLineageByParseSql2(SqlWithDataSourceKeyProperties sqlWithDataSourceKeyProperties) {
        List<DataSourceInfo> dataSourceInfos = dataSourceService.listByInfo(sqlWithDataSourceKeyProperties.getDataSourceKeyProperties());
        SqlWithDataSourceList sqlWithDataSourceList = new SqlWithDataSourceList();
        sqlWithDataSourceList.setSql(sqlWithDataSourceKeyProperties.getSql());
        sqlWithDataSourceList.setDataSourceInfos(dataSourceInfos);
        return addLineageByParseSql(sqlWithDataSourceList);
    }

    @Override
    public CatalogEntityLineageVO getLineageByFqn(Long datasourceId, String fqn, int upstreamDepth, int downstreamDepth) {
        CatalogEntityInstance catalogEntityInstance = catalogEntityInstanceService.getByDataSourceAndFQN(datasourceId, fqn);
        if (catalogEntityInstance == null) {
            throw new DataVinesServerException(Status.CATALOG_INSTANCE_IS_NULL_ERROR,fqn);
        }

        return getCatalogEntityLineageVO(catalogEntityInstance);
    }

    @NotNull
    private CatalogEntityLineageVO getCatalogEntityLineageVO(CatalogEntityInstance catalogEntityInstance) {
        Set<String> nodeSet = new HashSet<>();
        Set<String> edgeSet = new HashSet<>();

        CatalogEntityLineageVO catalogEntityLineageVO = new CatalogEntityLineageVO();
        CatalogEntityInstanceInfo currentEntityInstanceInfo = new CatalogEntityInstanceInfo();
        BeanUtils.copyProperties(catalogEntityInstance, currentEntityInstanceInfo);
        String fromUuid = catalogEntityInstance.getUuid();

        LineageEntityNodeInfo currentNode = new LineageEntityNodeInfo();
        BeanUtils.copyProperties(catalogEntityInstance, currentNode);
        CatalogEntityInstance databaseEntity = catalogEntityInstanceService.getParent(fromUuid);
        CatalogEntityInstanceInfo databaseEntityInstanceInfo = new CatalogEntityInstanceInfo();
        BeanUtils.copyProperties(databaseEntity, databaseEntityInstanceInfo);
        currentNode.setDatabase(databaseEntityInstanceInfo);

        DataSource dataSource = dataSourceService.getById(catalogEntityInstance.getDatasourceId());
        CatalogEntityInstanceInfo datasourceEntityInstanceInfo = new CatalogEntityInstanceInfo();
        datasourceEntityInstanceInfo.setType(dataSource.getType());
        datasourceEntityInstanceInfo.setDisplayName(dataSource.getName());
        datasourceEntityInstanceInfo.setUuid(dataSource.getUuid());
        datasourceEntityInstanceInfo.setId(dataSource.getId());
        List<CatalogEntityInstance> currentChildEntityInstances = catalogEntityInstanceService.getChildren(catalogEntityInstance.getUuid());
        if (CollectionUtils.isNotEmpty(currentChildEntityInstances)) {
            List<CatalogEntityInstanceInfo> columns = new ArrayList<>();
            for (CatalogEntityInstance childEntityInstance : currentChildEntityInstances) {
                CatalogEntityInstanceInfo columnEntityInstanceInfo = new CatalogEntityInstanceInfo();
                BeanUtils.copyProperties(childEntityInstance, columnEntityInstanceInfo);
                columns.add(columnEntityInstanceInfo);
            }
            currentNode.setColumns(columns);
        }
        currentNode.setDatasource(datasourceEntityInstanceInfo);

        catalogEntityLineageVO.setCurrentNode(currentNode);

        List<LineageEntityNodeInfo> nodes = new ArrayList<>();
        nodes.add(currentNode);

        List<LineageEntityEdgeInfo> edges = new ArrayList<>();

        List<CatalogEntityRel> downstreamRelList = list(new LambdaQueryWrapper<CatalogEntityRel>()
                .eq(CatalogEntityRel::getEntity1Uuid, fromUuid)
                .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
        if (CollectionUtils.isNotEmpty(downstreamRelList)) {
            for (CatalogEntityRel rel : downstreamRelList) {
                CatalogEntityInstance downstreamEntityInstance = catalogEntityInstanceService.getByUUID(rel.getEntity2Uuid());
                if (downstreamEntityInstance != null && !nodeSet.contains(downstreamEntityInstance.getUuid())) {
                    LineageEntityNodeInfo downstreamNode = new LineageEntityNodeInfo();
                    BeanUtils.copyProperties(downstreamEntityInstance, downstreamNode);

                    CatalogEntityInstance toDatabaseEntity = catalogEntityInstanceService.getParent(fromUuid);
                    CatalogEntityInstanceInfo toDatabaseEntityInstanceInfo = new CatalogEntityInstanceInfo();
                    BeanUtils.copyProperties(toDatabaseEntity, toDatabaseEntityInstanceInfo);
                    downstreamNode.setDatabase(toDatabaseEntityInstanceInfo);

                    DataSource toDataSource = dataSourceService.getById(catalogEntityInstance.getDatasourceId());
                    CatalogEntityInstanceInfo toDatasourceEntityInstanceInfo = new CatalogEntityInstanceInfo();
                    toDatasourceEntityInstanceInfo.setType(toDataSource.getType());
                    toDatasourceEntityInstanceInfo.setDisplayName(toDataSource.getName());
                    toDatasourceEntityInstanceInfo.setUuid(toDataSource.getUuid());
                    toDatasourceEntityInstanceInfo.setId(toDataSource.getId());
                    downstreamNode.setDatasource(toDatasourceEntityInstanceInfo);

                    List<CatalogEntityInstance> childEntityInstances = catalogEntityInstanceService.getChildren(downstreamEntityInstance.getUuid());
                    if (CollectionUtils.isNotEmpty(childEntityInstances)) {
                        List<CatalogEntityInstanceInfo> columns = new ArrayList<>();
                        for (CatalogEntityInstance childEntityInstance : childEntityInstances) {
                            CatalogEntityInstanceInfo columnEntityInstanceInfo = new CatalogEntityInstanceInfo();
                            BeanUtils.copyProperties(childEntityInstance, columnEntityInstanceInfo);
                            columns.add(columnEntityInstanceInfo);
                        }
                        downstreamNode.setColumns(columns);
                    }

                    List<CatalogEntityRel> downstreamRelList2 = list(new LambdaQueryWrapper<CatalogEntityRel>()
                            .eq(CatalogEntityRel::getEntity1Uuid, downstreamNode.getUuid())
                            .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
                    if (CollectionUtils.isNotEmpty(downstreamRelList2)) {
                        downstreamNode.setHasNextNode(true);
                    }

                    nodes.add(downstreamNode);
                    nodeSet.add(downstreamEntityInstance.getUuid());

                    if (!edgeSet.contains(currentEntityInstanceInfo.getUuid() + ":" + downstreamEntityInstance.getUuid())) {
                        LineageEntityEdgeInfo edgeInfo = new LineageEntityEdgeInfo();
                        CatalogEntityInstanceInfo fromEntity = new CatalogEntityInstanceInfo();
                        BeanUtils.copyProperties(currentEntityInstanceInfo, fromEntity);
                        CatalogEntityInstanceInfo toEntity = new CatalogEntityInstanceInfo();
                        BeanUtils.copyProperties(downstreamEntityInstance, toEntity);
                        edgeInfo.setFromEntity(fromEntity);
                        edgeInfo.setToEntity(toEntity);
                        edgeInfo.setUuid(fromEntity.getUuid() + ":" + toEntity.getUuid());

                        CatalogEntityLineageDetail lineageDetail = new CatalogEntityLineageDetail();
                        List<CatalogEntityColumnLineageDetail> childRelDetailList = new ArrayList<>();

                        if (CollectionUtils.isNotEmpty(currentNode.getColumns())
                                && CollectionUtils.isNotEmpty(downstreamNode.getColumns())) {
                            Map<String, CatalogEntityInstanceInfo> key2ColumnMap = new HashMap<>();
                            for (CatalogEntityInstanceInfo  column : currentNode.getColumns()) {
                                key2ColumnMap.put(column.getUuid(), column);
                            }

                            List<CatalogEntityInstanceInfo> columns = downstreamNode.getColumns();
                            for (CatalogEntityInstanceInfo column : columns) {
                                List<CatalogEntityRel> columnDownstreamRelList = list(new LambdaQueryWrapper<CatalogEntityRel>()
                                        .eq(CatalogEntityRel::getEntity2Uuid, column.getUuid())
                                        .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
                                if (CollectionUtils.isNotEmpty(columnDownstreamRelList)) {
                                    List<CatalogEntityInstanceInfo> fromChildren = columnDownstreamRelList
                                            .stream()
                                            .filter(x-> key2ColumnMap.containsKey(x.getEntity1Uuid()))
                                            .map(item -> key2ColumnMap.get(item.getEntity1Uuid())).collect(Collectors.toList());
                                    if (CollectionUtils.isNotEmpty(fromChildren)) {
                                        CatalogEntityColumnLineageDetail
                                                columnLineageDetail = new CatalogEntityColumnLineageDetail();
                                        columnLineageDetail.setFromChildren(fromChildren);
                                        columnLineageDetail.setToChild(column);
                                        childRelDetailList.add(columnLineageDetail);
                                    }
                                }
                            }
                        }

                        lineageDetail.setChildRelDetailList(childRelDetailList);
                        lineageDetail.setSourceType(LineageSourceType.descOf(rel.getSourceType()));
                        lineageDetail.setSqlQuery(rel.getRelatedScript());
                        edgeInfo.setLineageDetail(lineageDetail);
                        edges.add(edgeInfo);
                        edgeSet.add(fromEntity.getUuid() + ":" + toEntity.getUuid());
                    }
                }
            }
        }

        List<CatalogEntityRel> upstreamRelList = list(new LambdaQueryWrapper<CatalogEntityRel>()
                .eq(CatalogEntityRel::getEntity2Uuid, fromUuid)
                .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
        if (CollectionUtils.isNotEmpty(upstreamRelList)) {
            for (CatalogEntityRel rel : upstreamRelList) {
                CatalogEntityInstance upstreamEntityInstance = catalogEntityInstanceService.getByUUID(rel.getEntity1Uuid());
                if (upstreamEntityInstance != null && !nodeSet.contains(upstreamEntityInstance.getUuid())) {
                    LineageEntityNodeInfo upstreamNode = new LineageEntityNodeInfo();
                    BeanUtils.copyProperties(upstreamEntityInstance, upstreamNode);

                    CatalogEntityInstance fromDatabaseEntity = catalogEntityInstanceService.getParent(fromUuid);
                    CatalogEntityInstanceInfo fromDatabaseEntityInstanceInfo = new CatalogEntityInstanceInfo();
                    BeanUtils.copyProperties(fromDatabaseEntity, fromDatabaseEntityInstanceInfo);
                    upstreamNode.setDatabase(fromDatabaseEntityInstanceInfo);

                    DataSource fromDataSource = dataSourceService.getById(catalogEntityInstance.getDatasourceId());
                    CatalogEntityInstanceInfo fromDatasourceEntityInstanceInfo = new CatalogEntityInstanceInfo();
                    fromDatasourceEntityInstanceInfo.setType(fromDataSource.getType());
                    fromDatasourceEntityInstanceInfo.setDisplayName(fromDataSource.getName());
                    fromDatasourceEntityInstanceInfo.setUuid(fromDataSource.getUuid());
                    fromDatasourceEntityInstanceInfo.setId(fromDataSource.getId());
                    upstreamNode.setDatasource(fromDatasourceEntityInstanceInfo);

                    List<CatalogEntityInstance> childEntityInstances = catalogEntityInstanceService.getChildren(upstreamEntityInstance.getUuid());
                    if (CollectionUtils.isNotEmpty(childEntityInstances)) {
                        List<CatalogEntityInstanceInfo> columns = new ArrayList<>();
                        for (CatalogEntityInstance childEntityInstance : childEntityInstances) {
                            CatalogEntityInstanceInfo columnEntityInstanceInfo = new CatalogEntityInstanceInfo();
                            BeanUtils.copyProperties(childEntityInstance, columnEntityInstanceInfo);
                            columns.add(columnEntityInstanceInfo);
                        }
                        upstreamNode.setColumns(columns);
                    }

                    List<CatalogEntityRel> upstreamRelList2 = list(new LambdaQueryWrapper<CatalogEntityRel>()
                            .eq(CatalogEntityRel::getEntity2Uuid, upstreamNode.getUuid())
                            .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
                    if (CollectionUtils.isNotEmpty(upstreamRelList2)) {
                        upstreamNode.setHasNextNode(true);
                    }
                    nodes.add(upstreamNode);
                    nodeSet.add(upstreamEntityInstance.getUuid());

                    if (!edgeSet.contains(upstreamEntityInstance.getUuid() + ":" + currentEntityInstanceInfo.getUuid())) {
                        LineageEntityEdgeInfo edgeInfo = new LineageEntityEdgeInfo();
                        CatalogEntityInstanceInfo fromEntity = new CatalogEntityInstanceInfo();
                        BeanUtils.copyProperties(upstreamEntityInstance, fromEntity);
                        CatalogEntityInstanceInfo toEntity = new CatalogEntityInstanceInfo();
                        BeanUtils.copyProperties(currentEntityInstanceInfo, toEntity);
                        edgeInfo.setFromEntity(fromEntity);
                        edgeInfo.setToEntity(toEntity);
                        edgeInfo.setUuid(fromEntity.getUuid() + ":" + toEntity.getUuid());

                        CatalogEntityLineageDetail lineageDetail = new CatalogEntityLineageDetail();
                        List<CatalogEntityColumnLineageDetail> childRelDetailList = new ArrayList<>();

                        if (CollectionUtils.isNotEmpty(currentNode.getColumns())
                                && CollectionUtils.isNotEmpty(upstreamNode.getColumns())) {
                            Map<String, CatalogEntityInstanceInfo> key2ColumnMap = new HashMap<>();
                            for (CatalogEntityInstanceInfo  column : upstreamNode.getColumns()) {
                                key2ColumnMap.put(column.getUuid(), column);
                            }

                            List<CatalogEntityInstanceInfo> columns = currentNode.getColumns();
                            for (CatalogEntityInstanceInfo column : columns) {
                                List<CatalogEntityRel> columnDownstreamRelList = list(new LambdaQueryWrapper<CatalogEntityRel>()
                                        .eq(CatalogEntityRel::getEntity2Uuid, column.getUuid())
                                        .eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
                                if (CollectionUtils.isNotEmpty(columnDownstreamRelList)) {
                                    List<CatalogEntityInstanceInfo> fromChildren = columnDownstreamRelList
                                            .stream()
                                            .filter(x-> key2ColumnMap.containsKey(x.getEntity1Uuid()))
                                            .map(item -> key2ColumnMap.get(item.getEntity1Uuid())).collect(Collectors.toList());
                                    if (CollectionUtils.isNotEmpty(fromChildren)) {
                                        CatalogEntityColumnLineageDetail
                                                columnLineageDetail = new CatalogEntityColumnLineageDetail();
                                        columnLineageDetail.setFromChildren(fromChildren);
                                        columnLineageDetail.setToChild(column);
                                        childRelDetailList.add(columnLineageDetail);
                                    }
                                }
                            }
                        }

                        lineageDetail.setChildRelDetailList(childRelDetailList);
                        lineageDetail.setSourceType(LineageSourceType.descOf(rel.getSourceType()));
                        lineageDetail.setSqlQuery(rel.getRelatedScript());
                        edgeInfo.setLineageDetail(lineageDetail);
                        edges.add(edgeInfo);
                        edgeSet.add(fromEntity.getUuid() + ":" + toEntity.getUuid());
                    }
                }
            }
        }

        catalogEntityLineageVO.setNodes(nodes);
        catalogEntityLineageVO.setEdges(edges);

        return catalogEntityLineageVO;
    }

    @Override
    public CatalogEntityLineageVO getLineageByUUID(String uuid, int upstreamDepth, int downstreamDepth) {
        CatalogEntityInstance catalogEntityInstance = catalogEntityInstanceService.getByUUID(uuid);
        if (catalogEntityInstance == null) {
            throw new DataVinesServerException(Status.CATALOG_INSTANCE_IS_NULL_ERROR, uuid);
        }

        return getCatalogEntityLineageVO(catalogEntityInstance);
    }

    @Override
    public boolean deleteLineage(String fromUUID, String toUUID) {
        return remove(new LambdaQueryWrapper<CatalogEntityRel>().eq(CatalogEntityRel::getEntity1Uuid, fromUUID).eq(CatalogEntityRel::getEntity2Uuid, toUUID));
    }

    private boolean addLineage(String fromUUID, String toUUID, LineageSourceType sourceType, String sql) {
        List<CatalogEntityRel> relList = list(new LambdaQueryWrapper<CatalogEntityRel>()
                .eq(CatalogEntityRel::getEntity1Uuid, fromUUID)
                .eq(CatalogEntityRel::getEntity2Uuid, toUUID).eq(CatalogEntityRel::getType, EntityRelType.DOWNSTREAM.getDescription()));
        CatalogEntityRel rel;
        if (CollectionUtils.isEmpty(relList)) {
            rel = new CatalogEntityRel();
        } else {
            rel = relList.get(0);
        }
        rel.setEntity1Uuid(fromUUID);
        rel.setEntity2Uuid(toUUID);
        rel.setType(EntityRelType.DOWNSTREAM.getDescription());
        rel.setSourceType(sourceType.getDescription());
        rel.setRelatedScript(sql);
        rel.setUpdateBy(ContextHolder.getUserId());
        rel.setUpdateTime(LocalDateTime.now());
        if (CollectionUtils.isEmpty(relList)) {
            return save(rel);
        } else {
            return updateById(rel);
        }
    }

}
