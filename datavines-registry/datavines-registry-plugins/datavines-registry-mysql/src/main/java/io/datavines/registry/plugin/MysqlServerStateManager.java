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
package io.datavines.registry.plugin;

import io.datavines.common.utils.ConnectionUtils;
import io.datavines.common.utils.NetUtils;
import io.datavines.common.utils.Stopper;
import io.datavines.registry.api.Event;
import io.datavines.registry.api.ServerInfo;
import io.datavines.registry.api.SubscribeListener;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class MysqlServerStateManager {

    private Connection connection;

    private final ConcurrentHashMap<String, ServerInfo> liveServerMap = new ConcurrentHashMap<>();

    private Set<String> deadServers = new HashSet<>();

    private SubscribeListener subscribeListener;

    private final ServerInfo serverInfo;

    private final Properties properties;

    public MysqlServerStateManager(Connection connection, Properties properties) throws SQLException {
        this.connection = connection;
        this.properties = properties;
        serverInfo = new ServerInfo(NetUtils.getHost(), Integer.valueOf((String) properties.get("server.port")), new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis()));
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.scheduleAtFixedRate(new HeartBeater(), 2, 2, TimeUnit.SECONDS);
        executorService.scheduleAtFixedRate(new ServerChecker(), 5, 10, TimeUnit.SECONDS);
    }

    public void registry(SubscribeListener subscribeListener) throws SQLException {

        if (isExists(serverInfo)) {
            executeUpdate(serverInfo);
        } else {
            executeInsert(serverInfo);
        }

        liveServerMap.putAll(fetchServers());
        this.subscribeListener = subscribeListener;
        refreshServer();
    }

    public void unRegistry() throws SQLException {
        executeDelete(serverInfo);
    }

    public void refreshServer() throws SQLException {
        ConcurrentHashMap<String, ServerInfo> newServers = fetchServers();
        Set<String> offlineServer = new HashSet<>();
        if (newServers == null) {
            //do nothing
            return;
        }
        Set<String> onlineServer = new HashSet<>();
        newServers.forEach((k, v) -> {
            long updateTime = v.getUpdateTime().getTime();
            long now = System.currentTimeMillis();
            if (now - updateTime > 20000) {
                offlineServer.add(k);
            } else {
                onlineServer.add(k);
            }
        });

        liveServerMap.forEach((k, v) -> {
            if (newServers.get(k) == null) {
                offlineServer.add(k);
            }
        });

        //Get the latest list of servers, compare it with the existing cache,
        //get the list of lost heartbeat, and notify other servers to make fault tolerance
        offlineServer.forEach(x -> {
            if (!deadServers.contains(x) && !x.equals(serverInfo.getAddr())) {
                String[] values = x.split(":");
                try {
                    executeDelete(new ServerInfo(values[0], Integer.valueOf(values[1])));
                    liveServerMap.remove(x);
                } catch (SQLException e) {
                    log.error("delete server info error", e);
                }
                subscribeListener.notify(Event.builder().key(x).type(Event.Type.REMOVE).build());
            }
        });

        deadServers.addAll(offlineServer);

        //Judge whether there are any surviving servers in the dead server,
        //and if so, remove them from the dead server
        deadServers = deadServers
                .stream()
                .filter(x -> !onlineServer.contains(x))
                .collect(Collectors.toSet());

        onlineServer.forEach(x -> {
            if (liveServerMap.isEmpty()) {
                return;
            }
            if (liveServerMap.get(x) == null && !x.equals(serverInfo.getAddr())) {
                liveServerMap.put(x, newServers.get(x));
                subscribeListener.notify(Event.builder().key(x).type(Event.Type.ADD).build());
            }
        });
    }

    private void executeInsert(ServerInfo serverInfo) throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("insert into dv_server (host, port) values (?, ?)");
        preparedStatement.setString(1, serverInfo.getHost());
        preparedStatement.setInt(2, serverInfo.getServerPort());
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    private void executeUpdate(ServerInfo serverInfo) throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("update dv_server set update_time = ? where host = ? and port = ?");
        preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        preparedStatement.setString(2, serverInfo.getHost());
        preparedStatement.setInt(3, serverInfo.getServerPort());
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    private void executeDelete(ServerInfo serverInfo) throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("delete from dv_server where host = ? and port = ?");
        preparedStatement.setString(1, serverInfo.getHost());
        preparedStatement.setInt(2, serverInfo.getServerPort());
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    private boolean isExists(ServerInfo serverInfo) throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select * from dv_server where host= ? and port= ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        preparedStatement.setString(1, serverInfo.getHost());
        preparedStatement.setInt(2, serverInfo.getServerPort());
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet == null) {
            preparedStatement.close();
            return false;
        }
        boolean result = resultSet.first();
        resultSet.close();
        preparedStatement.close();
        return result;
    }

    private ConcurrentHashMap<String, ServerInfo> fetchServers() throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select * from dv_server");
        ResultSet resultSet = preparedStatement.executeQuery();

        if (resultSet == null) {
            preparedStatement.close();
            return null;
        }

        ConcurrentHashMap<String, ServerInfo> map = new ConcurrentHashMap<>();
        while (resultSet.next()) {
            String host = resultSet.getString("host");
            int port = resultSet.getInt("port");
            Timestamp updateTime = resultSet.getTimestamp("update_time");
            Timestamp createTime = resultSet.getTimestamp("create_time");
            map.put(host + ":" + port, new ServerInfo(host, port, createTime, updateTime));
        }
        resultSet.close();
        preparedStatement.close();
        return map;
    }

    public List<ServerInfo> getActiveServerList() {
        List<ServerInfo> activeServerList = new ArrayList<>();
        liveServerMap.forEach((k, v) -> {
            String[] values = k.split(":");
            if (values.length == 2) {
                activeServerList.add(v);
            }

        });
        return activeServerList;
    }

    class HeartBeater implements Runnable {

        @Override
        public void run() {
            if (Stopper.isRunning()) {
                try {
                    if (isExists(serverInfo)) {
                        executeUpdate(serverInfo);
                    } else {
                        executeInsert(serverInfo);
                    }
                } catch (SQLException e) {
                    log.error("heartbeat error", e);
                }
            }
        }
    }


    class ServerChecker implements Runnable {

        @Override
        public void run() {

            if (Stopper.isRunning()) {
                try {
                    refreshServer();
                } catch (SQLException e) {
                    log.error("server check error", e);
                }
            }
        }
    }

    private void checkConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionUtils.getConnection(properties);
        }
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}