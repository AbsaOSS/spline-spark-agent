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

FROM eclipse-temurin:11-jre-jammy

LABEL \
    vendor="ABSA" \
    copyright="2021 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Spline Agent for Apache Spark - Example suite"

# Install Tini
RUN apt-get update  \
    && apt-get install -y --no-install-recommends tini  \
    && rm -rf /var/lib/apt/lists/*

# Set working directry
WORKDIR /opt/spline-agent-examples

# Copy local directories to container
COPY target/libs target/libs
COPY target/classes target/classes
COPY data data
COPY run.sh .
COPY entrypoint.sh /entrypoint.sh

# ensure execute permissions on scripts
RUN chmod a+x run.sh
RUN chmod a+x /entrypoint.sh

# Bind environment variables
ENV SPLINE_PRODUCER_URL=
ENV SPLINE_MODE=ENABLED
ENV ON_INIT_FAILURE=BREAK
ENV DISABLE_SSL_VALIDATION=false

ENV HTTP_PROXY_HOST=
ENV HTTP_PROXY_PORT=
ENV HTTP_NON_PROXY_HOSTS=

ENTRYPOINT ["tini", "-g", "--", "sh", "/entrypoint.sh"]
