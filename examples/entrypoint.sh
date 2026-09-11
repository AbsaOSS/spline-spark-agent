#!/bin/sh

#
# Copyright 2023 ABSA Group Limited
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

exec ./run.sh \
    --all \
    -Dspline.lineageDispatcher=http \
    -Dspline.lineageDispatcher.http.producer.url="$SPLINE_PRODUCER_URL" \
    -Dspline.lineageDispatcher.http.disableSslValidation="$DISABLE_SSL_VALIDATION" \
    -Dspline.onInitFailure="$ON_INIT_FAILURE" \
    -Dspline.mode="$SPLINE_MODE" \
    -Dhttp.proxyHost="$HTTP_PROXY_HOST" \
    -Dhttp.proxyPort="$HTTP_PROXY_PORT" \
    -Dhttp.nonProxyHosts="$HTTP_NON_PROXY_HOSTS"
