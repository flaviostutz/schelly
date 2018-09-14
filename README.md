# schelly
Schelly is a backup tool focused on the scheduling stuff, so that the heavy lifting is performed by specialized storage/database tools. You can use any backup backend as soon as it is exposed by a simple REST API.

<p align="center">
  <img width="700" src="diagram1.png?raw=true">
</p>

The triggering and retainment of backups are based on the functional perception of backups, so you configure:
   - Triggering cron string: cron string that defines when a new backup will be created (by calling a backend backup webhook, as [schelly-restic](http://github.com/flaviostutz/schelly-restic), for example)
   - Retention policies: for how long do a backup must be retained? It depends on what the user needs when something goes wrong. In general, the more recent, more backups in time you need. By default, Schelly will try to keep something like (if a backup is outside this, the webhook for backup removal will be called):
       - the last 4 daily backups
       - the last 4 weekly backps
       - the last 3 monthly backups
       - the last 2 yearly backups

So, Schelly, based on those retention parameters, will call "POST /backup" or "DELETE /backup/{someid}" on backup backend server in order to maintain what we need as a backup that can save our souls! 

Some Schelly compatible backends are [Schelly Restic](https://github.com/flaviostutz/schelly-restic) and [Schelly Backy2](https://github.com/flaviostutz/schelly-backy2). If you wish to create your own webhook bridge, try to use [Schelly Webhook](https://github.com/flaviostutz/schelly-webhook), a Go library to help creating a Schelly compatible backup backend server.

Hope this can help you!

# Run

* copy docker-compose.yml

```
version: '3.5'

services:

  schelly:
    build:
      context: .
      target: IMAGE
    ports:
      - 8080:8080
    environment:
      - LOG_LEVEL=debug
      - BACKUP_NAME=test
      - WEBHOOK_URL=http://schelly-restic:7070/backups
      - BACKUP_CRON_STRING=0/3 * * * * *
      - RETENTION_MINUTELY=10

  schelly-restic:
    image: flaviostutz/schelly-restic
    ports:
      - 7070:7070
    environment:
      - RESTIC_PASSWORD=123
      - LOG_LEVEL=debug
```

* execute ```docker-compose up``` and see logs
* run ```curl localhost:7070/backups```

# ENV configurations

* BACKUP_NAME - name of the backup used as webhook prefix /[backup name]
* BACKUP_CRON_STRING - cron like string that configures the scheduling for the creation of new backups. if not defined, we will try to calculate an optimal schedule from the retetion policies
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
* GRACE\_TIME\_SECONDS - when trying to run a new backup task, if a previous task is still running because it didn't finish yet, check for this parameter. if time elapsed for the running task is greater than this parameter, try to cancel it by emitting a DELETE webhook and start the new task, else mark the new task as SKIPPED and keep the running task as is.

# REST API

  - ```GET /backups```
    - Query backups managed by Schelly
    - Query params:
       - 'status' - filter by status
       - 'tag' - filter by single tag
    - Request body: none
    - Request header: none
    - Response body: json 
     
      ```
        {
           id:{same id as returned by underlying webhook on backup creation},
           status:{backup-status}
           start_time:{time of backup trigger on webhook}
           end_time:{time of backup finish detection}
           custom_data:{data returned from webhook}
           tags: {array of tags}
        }
      ```
      - status must be one of:
          - 'running' - backup is not finished yet
          - 'available' - backup has completed successfuly
      
      - tags may be: 'minutely', 'hourly', 'daily', 'weekly', 'monthly', 'yearly'
      
    - Status code 201 if created successfuly

  - ```POST /backups```
    - Trigger a new backup now
    - Request body: none
    - Request header: none
    - Response body: json 
     
      ```
        {
           id:{same id as returned by underlying webhook on backup creation},
           status:{backup-status}
           start_time:{time of backup trigger on webhook}
           end_time:{time of backup finish detection}
           custom_data:{data returned from webhook}
        }
      ```
      - status must be always 'running' (check for backup completion later using GET /backups/{id})
      - status code must be 202 if backup request accepted


# Webhook spec

will be invoked when Schelly needs to create/delete a backup on a backend server

The webhook server must expose the following REST endpoints:

  - ```POST {webhook-url}```
    - Invoked when Schelly wants to trigger a new backup
    - Request body: json ```{webhook-create-body}```
    - Request header: ```{webhook-headers}```
    - Response body: json 
     
      ```
        {
           id:{alphanumeric-backup-id},
           status:{backup-status},
           message:{backend-message}
        }
      ```
      - status must be always 'running' (check for backup completion later using GET /backups/{id})
      - status code must be 202 if backup request accepted

  - ```GET {webhook-url}/{backup-id}```
    - Invoked when Schelly wants to query a specific backup instance
    - Request header: ```{webhook-headers}```
    - Response body: json
    
       ```
         {
           id:{id},
           data_id:{underlaying data id, or the same as id when not known yet},
           status:{backup-status},
           message:{backend message}
           size_mb:{backup-size-mbytes}
         }
       ```
    - Status code: 200 if found, 404 if not found
    - data_id: the backup creation webhook (POST /backups) must return immediately with an backup id that can be used for later cancellation (DELETE /backups/{id}). In many cases the backup webhook creates an id for the backup before the underlaying data backup is called or even finished (for example, when there are pre-backup commands or the backup storage mechanism only returns an id when finished). This field will have the underlaying data storage backup id, so that you will know what is the real reference in the underlaying storage when you need to restore or manage it.

  - ```DELETE {webhook-url}/{backup-id}```
    - Invoked when Schelly wants to trigger a new backup
    - Request body: json ```{webhook-delete-body}```
    - Request header: ```{webhook-headers}```
    - Response body: empty
    - Status code 200 if deleted successfuly

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
  * BACKUP\_CRON\_STRING    0 0 */4 ? * *
  * RETENTION_HOURLY      6
  * RETENTION_DAILY       0/3
  * RETENTION_MONTHLY     2@L
  * Trigger a backup every 4 hours and keep 6 of them, deleting older ones.
  * Mark the backup created on the last day of the month near 3am as 'monthly' and keep 2 of them.

# Monitoring

Schelly has a /metrics endpoint compatible with Prometheus

# Contribute

Please submit your issues and pull requests here!

# Some details

* Schelly will avoid performing concurrent invocations on webhook API

* If a backup fails (POST /backup webhook returns something different from 201), it will wait 5 seconds and retry again until 'grace time'

* If a backup deletion fails (DELETE /backup/{backupid} returns something different from 200), it will mark backup with status 'delete-error' and once a day will randomly retry to delete some of them.

# More resources

* https://github.com/flaviostutz/schelly-webhook
* https://github.com/flaviostutz/schelly-grafana
* https://github.com/flaviostutz/schelly-backy2
  * Clone this if you want to create your own Backup Repo
* https://github.com/flaviostutz/schelly-restic
