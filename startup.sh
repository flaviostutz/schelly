#!/bin/bash
set -e
# set -x

echo "Starting Schelly..."
schelly \
    --backup-name=$BACKUP_NAME \
    --backup-cron-string=$BACKUP_CRON_STRING \
    --webhook-url=$WEBHOOK_URL \
    --webhook-headers=$WEBHOOK_HEADERS \
    --webhook-create-body=$WEBHOOK_CREATE_BODY \
    --webhook-delete-body=$WEBHOOK_DELETE_BODY \
    --retention-secondly=$RETENTION_SECONDLY \
    --retention-minutely=$RETENTION_MINUTELY \
    --retention-hourly=$RETENTION_HOURLY \
    --retention-daily=$RETENTION_DAILY \
    --retention-weekly=$RETENTION_WEEKLY \
    --retention-monthly=$RETENTION_MONTHLY \
    --retention-yearly=$RETENTION_YEARLY \
    --log-level=$LOG_LEVEL
