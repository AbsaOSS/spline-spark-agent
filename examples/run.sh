#!/bin/bash

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

function ctrl_c() {
    echo 'Script interrupted by user'
    trap - SIGINT SIGTERM ## Clear the trap
    kill -- -$$ ## Sends SIGTERM to child/sub processes
}

print_line() {
    echo -e "\e[1m=======================================================================\e[0m"
    echo -e "\e[1mRunning $1 \e[0m"
    echo -e "\e[1m=======================================================================\e[0m"
}

if [ $# -eq 0 ]; then
    echo "Usage: $0 [--all [jvm_opts]... | [jvm_opts]... full.class.Name [prg-args]...]"
    echo "  --all            run all examples."
    echo "  full.class.Name  fully qualified class name to run."
    echo "  -jvm-opts        JVM options."
    echo "  -prg-args        program arguments."
    exit 1
fi

trap ctrl_c SIGINT SIGTERM

classpath=target/classes:$(echo target/libs/*.jar | tr ' ' ':')

if [ "$1" = "--all" ]; then
    while IFS= read -r -d '' file; do
        class=$(echo "${file#target/classes/}" | sed 's/\//./g' | sed 's/\.class$//')
        print_line "$class"
        java "${@:2}" -cp "$classpath" "$class"
    done < <(find target/classes -type f -name "*Job.class" -print0 | sort -z)
else
    class_pos=1
    for arg in "$@"
    do
        if [[ $arg != -* ]]
        then
            break
        fi
        class_pos=$((class_pos+1))
    done

    class=${!class_pos}
    options=("${@:1:$class_pos-1}")
    arguments=("${@:$class_pos+1}")

    print_line "$class"
    java "${options[@]}" -cp "$classpath" "$class" "${arguments[@]}"
fi
