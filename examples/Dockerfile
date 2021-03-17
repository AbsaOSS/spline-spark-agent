#
# Copyright 2021 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM maven:3.6.3-openjdk-8
COPY . /opt/spline-spark-agent

ENV SPLINE_PRODUCER_URL=http://spline.spline:8080/producer

RUN cd /opt/spline-spark-agent && \
    mvn install -DskipTests && \
    cd ./examples && \
    # it's for dry-run maven caching
    mvn test -P examples -D spline.mode=DISABLED

ENV WORKDIR=/opt/spline-spark-agent/examples

WORKDIR $WORKDIR

CMD mvn test -P examples -D spline.producer.url=$SPLINE_PRODUCER_URL 