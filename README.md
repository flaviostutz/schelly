# schelly
Schelly is a backup tool focused on the scheduling stuff. You can use any backup backend as a webhook.

# ENV configurations

* BACKUP_NAME - name of the backup used as webhook prefix /[backup name]
* BACKUP_CRON_STRING
* WEBHOOK_HEADERS - custom k=v comma separated list of http headers to be sent on webhook calls to backup backends
* WEBHOOK_CREATE_BODY - custom body to be sent to backup backend during new backup calls
* WEBHOOK_DELETE_BODY - custom body to be sent to backup backend during delete backup calls
* WEBHOOK_GRACE_TIME - Minimum time running backup task before trying to cancel it (by calling a /DELETE on the webhook)
* RETENTION_SECONDLY - retention config for seconds
* RETENTION_MINUTELY - retention config for minutes
* RETENTION_HOURLY - retention config for hours
* RETENTION_DAILY - retention config for days
* RETENTION_WEEKLY - retention config for weeks
* RETENTION_MONTHLY - retention config for months
* RETENTION_YEARLY - retention config for years
format "header1=contents1,header2=contents2"
* WEBHOOK_BODY - custom data to be sent as body for webhook calls to backup backends
* GRACE_TIME_SECONDS - when trying to run a new backup task, if a previous task is still running because it didn't finish yet, check for this parameter. if time elapsed for the running task is greater than this parameter, try to cancel it by emitting a DELETE webhook and start the new task, else mark the new task as SKIPPED and keep the running task as is.

#### Retention config:
  - *[retention count]@[reference]*, where
  - retention count: number of recent backups to be kept (older backups will be deleted)
  - reference: when this backup will be triggered in reference to the minor time part. 'L' denotes the greatest time in reference
  
#### Examples:

* Default backup
  * RETENTION_SECONDLY   0@L
  * RETENTION_MINUTELY   0@L
  * RETENTION_HOURLY     0@L
  * RETENTION_DAILY      4@L
  * RETENTION_WEEKLY     3@L
  * RETENTION_MONTHLY    3@L
  * RETENTION_YEARLY     2@L
  * Every day, at hour 0, minute 0, a daily backup will be triggered. Four of these backups will be kept. 
  * At the last day of the week (SAT), the daily backup will be marked as a weekly backup. Three of these weekly backups will be kept. 
  * At the last day of the month, the last hourly backup of the day will be marked as a monthly backup. Three of these monthly backups will be kept. 
  * At the last month of the year, at the last day of the month, the daily backup will be marked as yearly backup. Two of these labeled backups will be kept too.

* Simple daily backups
  * RETENTION_DAILY       7
  * The backup will be triggered every day at 23h59min (L) and 7 backups will be kept. On the 8th day, the first backup will be deleted

* Every 4 hours backups
  * BACKUP_CRON_STRING    0 0 */4 ? * *
  * RETENTION_HOURLY      6
  * RETENTION_DAILY       0/3
  * RETENTION_MONTHLY     2@L
  * Trigger a backup every 4 hours and keep 6 of them, deleting older ones.
  * Mark the backup created on the last day of the month near 3am as 'monthly' and keep 2 of them.

