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
