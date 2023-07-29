#!/bin/bash

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
  echo "Usage: $0 [--all | full.class.Name] [-jvm-option]..."
  echo "  --all            run all examples."
  echo "  full.class.Name  fully qualified class name to run."
  echo "  -jvm-option      options and values passed to the java command."
  exit 1
fi

trap ctrl_c SIGINT SIGTERM

CLASSPATH=target/classes:$(echo target/libs/*.jar | tr ' ' ':')

if [ "$1" = "--all" ]; then
  while IFS= read -r -d '' file; do
    class=$(echo "${file#target/classes/}" | sed 's/\//./g' | sed 's/\.class$//')
    print_line "$class"
    java "${@:2}" -cp "$CLASSPATH" "$class"
  done < <(find target/classes -type f -name "*Job.class" -print0 | sort -z)
else
  print_line "$1"
  java "${@:2}" -cp "$CLASSPATH" "$1"
fi
