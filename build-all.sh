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
# Build Spline Agent artifacts for all supported Scala versions and install them to local maven repository.
#

DEFAULT_SCALA_VERSION=2.12
SCALA_VERSIONS=(2.11 2.12 2.13)

BASE_DIR=$(dirname "$0")
MODULE_DIRS=$(find "$BASE_DIR" -type f -name "pom.xml" -exec dirname {} \;)
MVN_EXEC="mvn"

# Scala 2.13 is built against Spark 4, which requires JDK 17. Point JAVA_HOME_17 at one,
# or rely on java_home to find it on a Mac.
if [ -z "${JAVA_HOME_17:-}" ] && [ -x /usr/libexec/java_home ]; then
  JAVA_HOME_17=$(/usr/libexec/java_home -v 17 2>/dev/null)
fi

print_title() {
  echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
  echo "                           $1                                                  "
  echo "░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░"
}

cross_build() {
  bin_ver=$1

  # checked before the POMs are rewritten, or a machine without JDK 17 is left with 2.13 POMs
  if [ "$bin_ver" = "2.13" ] && [ -z "${JAVA_HOME_17:-}" ]; then
    echo "Set JAVA_HOME_17 to a JDK 17 or 21 installation to build Scala 2.13" >&2
    exit 1
  fi

  # pre-cleaning
  for dir in $MODULE_DIRS; do
    rm -rf "$dir"/target
  done

  print_title "Switching to Scala $bin_ver"
  $MVN_EXEC scala-cross-build:change-version -Pscala-"$bin_ver"

  print_title "Building with Scala $bin_ver"
  if [ "$bin_ver" = "2.13" ]; then
    # Scala 2.13 is only supported together with Spark 4.0, which requires JDK 17
    JAVA_HOME="$JAVA_HOME_17" \
    MAVEN_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED" \
    $MVN_EXEC install -Pscala-"$bin_ver",spark-4.0 -DskipTests -T 1C || exit 1
  else
    $MVN_EXEC install -Pscala-"$bin_ver" -DskipTests -T 1C || exit 1
  fi
}

# -------------------------------------------------------------------------------

for v in "${SCALA_VERSIONS[@]}"; do
  cross_build "$v"
done

print_title "Restoring POM-files"

# bundle-4.0 is only a module of the scala-2.13 profile, so the command below doesn't reach it.
# It goes first, while the root POM is still the 2.13 one it names as its parent.
(cd "$BASE_DIR"/bundle-4.0 && $MVN_EXEC -N scala-cross-build:change-version -Pscala-"${DEFAULT_SCALA_VERSION}") || exit 1

$MVN_EXEC scala-cross-build:change-version -Pscala-"${DEFAULT_SCALA_VERSION}" || exit 1

# remove backup files
for dir in $MODULE_DIRS; do
  rm -f "$dir"/pom.xml.bkp
done
