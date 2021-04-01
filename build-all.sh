#!/bin/bash
# ------------------------------------------------------------------------
# Copyright 2020 ABSA Group Limited
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
# ------------------------------------------------------------------------
#
# THIS SCRIPT IS INTENDED FOR LOCAL DEV USAGE ONLY
#

SCALA_VERSIONS=(2.11 2.12)
BASE_DIR=$(dirname "$0")

log() {
  echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
  echo "                           $1                                                  "
  echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
}

cross_build() {
  bin_ver=$1
  log "Building with Scala $bin_ver"
  find "$BASE_DIR"/target/* -type d -exec rm -rf {} \; 2> /dev/null

  mvn scala-cross-build:change-version -Pscala-$bin_ver
  mvn install -Pscala-$bin_ver
}

# -------------------------------------------------------------------------------

for v in "${SCALA_VERSIONS[@]}"; do
  cross_build "$v"
done

log "Restoring POM-files"
scala_profiles=$(for v in ${SCALA_VERSIONS[*]}; do echo "-Pscala-$v"; done)
mvn scala-cross-build:restore-version $scala_profiles
