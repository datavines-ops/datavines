FROM openjdk:8

MAINTAINER 735140144

ARG VERSION=1.0.0-SNAPSHOT
ARG FLINK_VERSION=1.13.6
ARG SCALA_VERSION=2.11

ENV TZ=Asia/Shanghai \
    LANG=zh_CN.UTF-8 \
    FLINK_HOME=/opt/flink \
    PATH=$PATH:/opt/flink/bin

WORKDIR /opt

# Install tini and wget for proper signal handling and downloading
RUN apt-get update && \
    apt-get install -y tini wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and setup Flink for engine support
RUN wget -P /opt https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz && \
    tar -xzf flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz && \
    mv flink-${FLINK_VERSION} flink && \
    rm flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz && \
    mkdir -p $FLINK_HOME/plugins/metrics-prometheus && \
    cp $FLINK_HOME/opt/flink-metrics-prometheus-${FLINK_VERSION}.jar $FLINK_HOME/plugins/metrics-prometheus/ && \
    mkdir -p $FLINK_HOME/logs

# Copy and extract datavines
COPY datavines-dist/target/datavines-${VERSION}-bin.tar.gz .
RUN tar -zxvf datavines-${VERSION}-bin.tar.gz && \
    mv datavines-${VERSION}-bin datavines && \
    rm -rf datavines-${VERSION}-bin.tar.gz

# Fix script permissions and line endings
RUN chmod +x datavines/bin/datavines-daemon.sh && \
    sed -i 's/\r//g' datavines/bin/datavines-daemon.sh

# Set up minimal Flink configuration
RUN echo "jobmanager.memory.process.size: 1600m" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "taskmanager.memory.process.size: 1728m" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "taskmanager.numberOfTaskSlots: 4" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "parallelism.default: 2" >> $FLINK_HOME/conf/flink-conf.yaml

# Expose Datavines port (primary) and Flink ports (auxiliary)
EXPOSE 5600 8081 6123

# Use tini as init process
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["datavines/bin/datavines-daemon.sh", "start_container", ""]
