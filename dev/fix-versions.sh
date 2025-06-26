#!/usr/bin/env bash
#
# Copyright 2025 ABSA Group Limited
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

set -eu

# -----------------------------------------------------------------------------
# This script fixes the missing versions in the dependencyManagement section
# of a Maven POM file for a specific bundle module.
#
# Usage:
#   ./fix-versions.sh <bundle-module> <maven-profile>
#
# E.g.
#   ./fix-versions.sh bundle-2.4 scala-2.12
#
# -----------------------------------------------------------------------------

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <bundle-module> <maven-profile>"
  exit 1
fi

MODULE_DIR=$1
PROFILE=$2
INPUT_POM="${MODULE_DIR}/pom.xml"
OUTPUT_POM="${MODULE_DIR}/pom.xml"

if [[ ! -f "${INPUT_POM}" ]]; then
  echo "Error: Input POM file not found: ${INPUT_POM}"
  exit 1
fi

# collect coords of provided deps without version
readarray -t COORDS < <(
  xmlstarlet sel \
    -N x="http://maven.apache.org/POM/4.0.0" \
    -t -m "//x:dependencyManagement/x:dependencies/x:dependency[x:scope='provided' and not(x:version)]" \
    -v "concat(x:groupId,':',x:artifactId)" -n "$INPUT_POM"
)

# get resolved versions
TMP_DEPS=$(mktemp)
mvn -q \
    -P"$PROFILE" dependency:list \
    -DincludeScope=provided \
    -DoutputFile="$TMP_DEPS" \
    -pl "$MODULE_DIR"

# extract only dependency lines (skip header and blank lines)
CLEAN_DEPS=$(mktemp)
grep -E '^[[:space:]]' "$TMP_DEPS" > "$CLEAN_DEPS"

# prepare output
if [[ "$INPUT_POM" != "$OUTPUT_POM" ]]; then
  cp "$INPUT_POM" "$OUTPUT_POM"
fi

# inject missing <version> (and <classifier>) for each coord
for coord in "${COORDS[@]}"; do
  groupId=${coord%%:*}
  artifactId=${coord#*:}

  # pick the first matching line
  line=$(grep -E "^[[:space:]]*${groupId}:${artifactId}:" "$CLEAN_DEPS" | head -1)

  if [[ -z "$line" ]]; then
    echo "Removed: $groupId:$artifactId"
    xmlstarlet ed -L -N x="http://maven.apache.org/POM/4.0.0" \
      -d "/x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='$groupId' and x:artifactId='$artifactId']" \
      "$OUTPUT_POM"
    continue
  fi

  # strip leading spaces and split on colon
  line="${line#"${line%%[![:space:]]*}"}"
  IFS=':' read -ra parts <<< "$line"

  if [[ ${#parts[@]} -eq 5 ]]; then
    classifier=""
    version=${parts[3]}
  elif [[ ${#parts[@]} -eq 6 ]]; then
    classifier=${parts[3]}
    version=${parts[4]}
  else
    echo "Warning: unexpected format: $line" >&2
    continue
  fi

  echo "Fixed: $groupId:$artifactId -> $version${classifier:+ (classifier $classifier)}"
  # insert version
  xmlstarlet ed -L -N x="http://maven.apache.org/POM/4.0.0" \
    -s "/x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='$groupId' and x:artifactId='$artifactId']" \
    -t elem -n version -v "$version" \
    "$OUTPUT_POM"

  # insert classifier if present
  if [[ -n "$classifier" ]]; then
    xmlstarlet ed -L -N x="http://maven.apache.org/POM/4.0.0" \
      -s "/x:project/x:dependencyManagement/x:dependencies/x:dependency[x:groupId='$groupId' and x:artifactId='$artifactId']" \
      -t elem -n classifier -v "$classifier" \
      "$OUTPUT_POM"
  fi
done

rm "$TMP_DEPS" "$CLEAN_DEPS"
printf "\n$OUTPUT_POM written.\n"
