#!/bin/bash

LOG_DIR="/opt/airflow/logs/drone_data"
mkdir -p $LOG_DIR

LOG_FILE="$LOG_DIR/drone_data_$(date +'%Y%m%d').log"

log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> $LOG_FILE
}

SOURCE_DIR="/opt/bitnami/spark/data"
TARGET_DIR="/opt/airflow/data"
DATA_FILE="flights.csv"

log "Starting drone data collection process"

if [ -f "$SOURCE_DIR/$DATA_FILE" ]; then
    log "Data file found: $SOURCE_DIR/$DATA_FILE"
    
    mkdir -p $TARGET_DIR
    cp "$SOURCE_DIR/$DATA_FILE" "$TARGET_DIR/$DATA_FILE"
    
    if [ $? -eq 0 ]; then
        log "Data file successfully copied to $TARGET_DIR/$DATA_FILE"
    else
        log "Error: Failed to copy data file"
        exit 1
    fi
else
    log "Error: Data file not found in $SOURCE_DIR"
    exit 1
fi

log "Drone data collection process completed successfully"
exit 0