#!/bin/bash
set -e
set -x

echo "Starting Schelly..."
schelly \
    --backup-name=$BACKUP_NAME \
    --backup-cron-string="$BACKUP_CRON_STRING" \
    --retention-cron-string="$RETENTION_CRON_STRING" \
    --conductor-api-url=$CONDUCTOR_API_URL \
    --backup-timeout=$BACKUP_TIMEOUT \
    --webhook-grace-time=$WEBHOOK_GRACE_TIME \
    --data-dir="$DATA_DIR" \
    --log-level=$LOG_LEVEL

