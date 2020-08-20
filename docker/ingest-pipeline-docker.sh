#!/bin/bash

# Remove newlines and leading/trailing slashes if present in that build_number file
function export_build_num() {
    export INGEST_PIPELINE_BUILD_NUM=$(tr -d "\n\r" < ../build_number | xargs)
    echo "INGEST_PIPELINE_BUILD_NUM: $INGEST_PIPELINE_BUILD_NUM"
}

if [[ "$1" != "localhost" ]]; then
    echo "Unknown build environment '$1', specify: localhost"
else
    if [[ "$2" != "check" && "$2" != "config" && "$2" != "build" && "$2" != "start" && "$2" != "stop" && "$2" != "down" ]]; then
        echo "Unknown command '$2', specify one of the following: check|config|build|start|stop|down"
    else
        if [ "$2" = "check" ]; then
            # Bash array
            config_paths=(
                '../ingest-pipeline/src/ingest-pipeline/instance/app.cfg'
            )

            for pth in "${config_paths[@]}"; do
                if [ ! -e $pth ]; then
                    echo "Missing $pth"
                    exit -1
                fi
            done

            # The `absent_or_newer` checks if the copied src at docker/some-api/src directory exists 
            # and if the source src directory is newer. 
            # If both conditions are true `absent_or_newer` writes an error message 
            # and causes hubmap-docker.sh to exit with an error code.
            absent_or_newer entity-api/src ../src

            echo 'Checks complete, all good :)'
        elif [ "$2" = "config" ]; then
            export_build_num
            docker-compose -p ingest-pipeline -f docker-compose.yml -f docker-compose.$1.yml config
        elif [ "$2" = "build" ]; then
            export_build_num
            docker-compose -p ingest-pipeline -f docker-compose.yml -f docker-compose.$1.yml build
        elif [ "$2" = "start" ]; then
            export_build_num
            docker-compose -p ingest-pipeline -f docker-compose.yml -f docker-compose.$1.yml up -d
        elif [ "$2" = "stop" ]; then
            export_build_num
            docker-compose -p ingest-pipeline -f docker-compose.yml -f docker-compose.$1.yml stop
        elif [ "$2" = "down" ]; then
            export_build_num
            docker-compose -p ingest-pipeline -f docker-compose.yml -f docker-compose.$1.yml down
        fi
    fi
fi
