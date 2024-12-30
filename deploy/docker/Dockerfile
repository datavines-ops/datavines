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

# Install required packages
RUN apt-get update && \
    apt-get install -y tini wget sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and setup Flink for engine support
RUN wget --no-check-certificate https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz && \
    tar -xzf flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz && \
    mv flink-${FLINK_VERSION} flink && \
    rm flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz && \
    mkdir -p $FLINK_HOME/logs

# Copy and extract datavines
COPY datavines-dist/target/datavines-${VERSION}-bin.tar.gz .
RUN tar -zxvf datavines-${VERSION}-bin.tar.gz && \
    mv datavines-${VERSION}-bin datavines && \
    rm -rf datavines-${VERSION}-bin.tar.gz

# Fix script permissions and line endings
RUN chmod +x datavines/bin/datavines-daemon.sh && \
    sed -i 's/\r//g' datavines/bin/datavines-daemon.sh && \
    # Copy Flink dependencies
    cp datavines/lib/datavines-flink-core*.jar $FLINK_HOME/lib/ && \
    # Create necessary directories with proper permissions
    mkdir -p /tmp/datavines/exec/job/flink && \
    chmod -R 777 /tmp/datavines

# Set up minimal Flink configuration
RUN echo "jobmanager.memory.process.size: 1600m" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "taskmanager.memory.process.size: 1728m" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "taskmanager.numberOfTaskSlots: 4" >> $FLINK_HOME/conf/flink-conf.yaml && \
    echo "parallelism.default: 2" >> $FLINK_HOME/conf/flink-conf.yaml

# Create datavines user and set permissions
RUN useradd -m -s /bin/bash datavines && \
    echo "datavines ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \
    chown -R datavines:datavines /opt/datavines /opt/flink /tmp/datavines

USER datavines

# Expose Datavines port (primary) and Flink ports (auxiliary)
EXPOSE 5600 8081 6123

# Use tini as init process
CMD ["/usr/bin/tini", "--", "datavines/bin/datavines-daemon.sh", "start_container", ""]
