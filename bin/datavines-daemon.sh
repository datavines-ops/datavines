#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

usage="Usage: datavines-daemon.sh (start|start_container|start_with_jmx|stop|restart_with_jmx|status) <''|mysql>"

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

startStop=$1
shift
profile=$1
shift

springProfileActive=

if [ -n "$profile" ]; then
	if [ "$profile" = "mysql" ]; then
	  springProfileActive="-Dspring.profiles.active=mysql"
	else
	  echo "Error: No profile named \`$profile' was found."
	  exit 1
	fi
fi

echo "Begin $startStop DataVinesServer $profile......"

BIN_DIR=`dirname $0`
BIN_DIR=`cd "$BIN_DIR"; pwd`
DATAVINES_HOME=$BIN_DIR/..

source /etc/profile

export JAVA_HOME=$JAVA_HOME
export HOSTNAME=`hostname`

export DATAVINES_PID_DIR=$DATAVINES_HOME/pid
export DATAVINES_LOG_DIR=$DATAVINES_HOME/logs
export DATAVINES_CONF_DIR=$DATAVINES_HOME/conf
export DATAVINES_LIB_JARS=$DATAVINES_HOME/libs/*

export DATAVINES_OPTS="-server -Xmx16g -Xms1g -XX:+UseG1GC -XX:G1HeapRegionSize=8M"
export STOP_TIMEOUT=5

if [ ! -d "$DATAVINES_LOG_DIR" ]; then
  mkdir $DATAVINES_LOG_DIR
fi

log=$DATAVINES_LOG_DIR/datavines-server-$HOSTNAME.out
pid=$DATAVINES_PID_DIR/datavines-server.pid

cd $DATAVINES_HOME

LOG_FILE="-Dlogging.config=classpath:server-logback.xml $springProfileActive"
CLASS=io.datavines.server.DataVinesServer

# JMX path
JMX="-javaagent:$DATAVINES_HOME/libs/jmx_prometheus_javaagent-0.20.0.jar=10010:$DATAVINES_CONF_DIR/jmx/jmx_exporter_config.yaml"

case $startStop in
  (start)
    [ -w "$DATAVINES_PID_DIR" ] ||  mkdir -p "$DATAVINES_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo "DataVinesServer running as process `cat $pid`. Stop it first."
        exit 1
      fi
    fi

    echo "Starting DataVinesServer, logging to $log ..."

    exec_command="$LOG_FILE $DATAVINES_OPTS -classpath $DATAVINES_CONF_DIR:$DATAVINES_LIB_JARS $CLASS"

    echo "nohup $JAVA_HOME/bin/java $exec_command > $log 2>&1 &"
    nohup $JAVA_HOME/bin/java $exec_command > $log 2>&1 &
    echo $! > $pid
    ;;

  (start_container)
    [ -w "$DATAVINES_PID_DIR" ] ||  mkdir -p "$DATAVINES_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo "DataVinesServer running as process `cat $pid`. Stop it first."
        exit 1
      fi
    fi

    echo "Starting DataVinesServer, logging to $log ..."

    exec_command="$LOG_FILE $DATAVINES_OPTS -classpath $DATAVINES_CONF_DIR:$DATAVINES_LIB_JARS $CLASS"

    echo "$JAVA_HOME/bin/java $exec_command"
    $JAVA_HOME/bin/java $exec_command
    echo $! > $pid
    ;;

  (start_with_jmx)
    [ -w "$DATAVINES_PID_DIR" ] ||  mkdir -p "$DATAVINES_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo "DataVinesServer running as process `cat $pid`. Stop it first."
        exit 1
      fi
    fi

    echo "Starting DataVinesServer, logging to $log ..."

    exec_command="$LOG_FILE $DATAVINES_OPTS ${JMX} -classpath $DATAVINES_CONF_DIR:$DATAVINES_LIB_JARS $CLASS"

    echo "nohup $JAVA_HOME/bin/java $exec_command > $log 2>&1 &"
    nohup $JAVA_HOME/bin/java $exec_command > $log 2>&1 &
    echo $! > $pid
    ;;

  (stop)
      if [ -f $pid ]; then
        TARGET_PID=`cat $pid`
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "Stopping DataVinesServer..."
          kill $TARGET_PID
          sleep $STOP_TIMEOUT
          if kill -0 $TARGET_PID > /dev/null 2>&1; then
            echo "DataVinesServer did not stop gracefully after $STOP_TIMEOUT seconds: killing with kill -9"
            kill -9 $TARGET_PID
          fi
        else
          echo "No DataVinesServer to stop."
        fi
        rm -f $pid
      else
        echo "No DataVinesServer to stop."
      fi
      ;;

  (status)
    if [ -f $pid ]; then
      echo ""
      echo "Service DataVinesServer is running. It's pid=${pid}"
      echo ""
    else
      echo ""
      echo "Service DataVinesServer is not running!"
      echo ""
      exit 1
    fi
    ;;

  (restart_with_jmx)
    echo ""
    stop
    start_with_jmx
    echo "........................................Restart with Jmx Successfully........................................"
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac

echo "End $startStop DataVinesServer $profile."
