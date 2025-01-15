#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM openjdk:8

MAINTAINER 735140144

ARG VERSION=1.0.0-SNAPSHOT

ENV TZ=Asia/Shanghai
ENV LANG=zh_CN.UTF-8

WORKDIR /opt

RUN apt-get update && \
    apt-get install -y tini && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY datavines-dist/target/datavines-${VERSION}-bin.tar.gz .

RUN tar -zxvf datavines-${VERSION}-bin.tar.gz && \
    mv datavines-${VERSION}-bin datavines && \
    rm -rf datavines-${VERSION}-bin.tar.gz

RUN chmod +x datavines/bin/datavines-daemon.sh && sed -i 's/\r//g' datavines/bin/datavines-daemon.sh

EXPOSE 5600

CMD ["/usr/bin/tini", "--", "datavines/bin/datavines-daemon.sh", "start_container", ""]