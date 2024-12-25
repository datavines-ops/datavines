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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.datavines.common.utils.ConnectionUtils;
import io.datavines.common.utils.NetUtils;
import io.datavines.common.utils.ThreadUtils;
import io.datavines.registry.api.ServerInfo;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MysqlMutex {

    public static final long LOCK_ACQUIRE_INTERVAL = 1000;

    private final long expireTimeWindow = 600;

    private Connection connection;

    private final Properties properties;

    private final ServerInfo serverInfo;

    private final ConcurrentHashMap<String, RegistryLock> lockHoldMap;

    public MysqlMutex(Connection connection, Properties properties) throws SQLException {
        this.connection = connection;
        this.properties = properties;
        this.serverInfo = new ServerInfo(NetUtils.getHost(), Integer.valueOf((String) properties.get("server.port")));
        this.lockHoldMap = new ConcurrentHashMap<>();
        ScheduledExecutorService lockTermUpdateThreadPool = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("RegistryLockRefreshThread").setDaemon(true).build());

        lockTermUpdateThreadPool.scheduleWithFixedDelay(
                new LockTermRefreshTask(lockHoldMap),
                2,
                2,
                TimeUnit.SECONDS);

        clearExpireLock();
    }

    public boolean acquire(String lockKey, long time) {
        // 尝试插入，如果抛出异常，说明锁被占用，会休眠一段时间再去获取锁，直到获取到锁
        RegistryLock lock = lockHoldMap.computeIfAbsent(lockKey, key -> {
            RegistryLock registryLock = null;
            int count = 1;
            if (time > 0) {
                count = Math.max(1, (int) (time * 1000 / LOCK_ACQUIRE_INTERVAL));
            }
            while (count > 0) {
                try {
                    registryLock = executeInsert(key);
                    log.debug("Acquire the lock success, {}", key);
                    count = 0;
                } catch (SQLException e) {
                    log.error("Acquire the lock error, {}, try again!", e.getLocalizedMessage());
                    try {
                        clearExpireLock();
                    } catch (SQLException ex) {
                        log.error("clear expire lock error : ", ex);
                    }
                    ThreadUtils.sleep(LOCK_ACQUIRE_INTERVAL);
                    count--;
                }
            }

            return registryLock;
        });

        return lock != null;
    }

    public boolean release(String lockKey) throws SQLException {
        RegistryLock registryLock = lockHoldMap.get(lockKey);
        if (registryLock != null) {
            try {
                executeDelete(lockKey);
                lockHoldMap.remove(lockKey);
            } catch (SQLException e) {
                log.error(String.format("Release lock: %s error", lockKey), e);
                return false;
            }
        }

        return true;
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private RegistryLock executeInsert(String key) throws SQLException {
        checkConnection();
        Timestamp updateTime = new Timestamp(System.currentTimeMillis());
        PreparedStatement preparedStatement = connection.prepareStatement("insert into dv_registry_lock (lock_key,lock_owner,update_time) values (?,?,?)");
        preparedStatement.setString(1, key);
        preparedStatement.setString(2, this.serverInfo.getAddr());
        preparedStatement.setTimestamp(3, updateTime);
        preparedStatement.executeUpdate();
        preparedStatement.close();
        return new RegistryLock(key, this.serverInfo.getAddr(), updateTime);
    }

    private RegistryLock executeUpdate(String key) throws SQLException {
        checkConnection();
        Timestamp updateTime = new Timestamp(System.currentTimeMillis());
        PreparedStatement preparedStatement = connection.prepareStatement("update dv_registry_lock set update_time = ? where lock_key = ?");
        preparedStatement.setTimestamp(1, updateTime);
        preparedStatement.setString(2, key);
        preparedStatement.executeUpdate();
        preparedStatement.close();
        return new RegistryLock(key, this.serverInfo.getAddr(), updateTime);
    }

    private void executeDelete(String key) throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("delete from dv_registry_lock where lock_key = ?");
        preparedStatement.setString(1, key);
        if (preparedStatement.executeUpdate() > 0) {
            lockHoldMap.remove(key);
        }
        preparedStatement.close();
    }

    private boolean isExists(String key, ServerInfo serverInfo) throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select * from dv_registry_lock where lock_key=?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        preparedStatement.setString(1, key);
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

    private void clearExpireLock() throws SQLException {
        checkConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("delete from dv_registry_lock where update_time < ?");
        preparedStatement.setTimestamp(1, new Timestamp(System.currentTimeMillis() - expireTimeWindow));
        preparedStatement.executeUpdate();
        preparedStatement.close();
        // 将超时的lockKey移除掉
        lockHoldMap.values().removeIf((v -> v.getUpdateTime().getTime() < (System.currentTimeMillis() - expireTimeWindow)));
    }

    private void checkConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionUtils.getConnection(properties);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    class LockTermRefreshTask implements Runnable {

        private final Map<String, RegistryLock> lockHoldMap;

        @Override
        public void run() {
            try {
                if (lockHoldMap.isEmpty()) {
                    return;
                }

                List<String> lockKeys = new ArrayList<>();
                for (RegistryLock lock : lockHoldMap.values()) {
                    if (lock != null) {
                        lockKeys.add(lock.getLockKey());
                    }
                }
                lockKeys.forEach(lockKey -> {
                    try {
                        RegistryLock registryLock = executeUpdate(lockKey);
                        lockHoldMap.put(lockKey, registryLock);
                    } catch (SQLException e) {
                        log.warn("Update the lock: {} term failed.", lockKey);
                    }
                });

                clearExpireLock();
            } catch (Exception e) {
                log.error("Update lock term error", e);
            }
        }
    }
}
