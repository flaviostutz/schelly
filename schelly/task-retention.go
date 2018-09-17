package main

import (
	"strconv"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
)

//METRICS
var retentionTasksCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_retention_tasks_total",
	Help: "Total retention tasks triggered",
})

var retentionBackupsDeleteSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_retention_backup_delete_success_total",
	Help: "Total retention backups deleted with success",
})

var retentionBackupsDeleteErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_retention_backup_delete_error_total",
	Help: "Total retention backups deleted with error",
})

var retentionBackupsRetriesCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_retention_backup_delete_retries_total",
	Help: "Total retention backup delete retries",
})

var runningTask = false

//avoid doing webhook operations in parallel
var avoidRetentionLock = &sync.Mutex{}

func initRetention() {
	prometheus.MustRegister(retentionTasksCounter)
	prometheus.MustRegister(retentionBackupsDeleteSuccessCounter)
	prometheus.MustRegister(retentionBackupsDeleteErrorCounter)
	prometheus.MustRegister(retentionBackupsRetriesCounter)
}

func runRetentionTask() {
	if runningTask {
		logrus.Debug("runRetentionTask already running. skipping new task creation")
		return
	} else {
		runningTask = true
	}
	triggerRetentionTask()
	runningTask = false
}

func triggerRetentionTask() {
	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP RETENTION MANAGEMENT")
	retentionTasksCounter.Inc()

	avoidRetentionLock.Lock()

	tagAllBackups()

	logrus.Debugf("Retention policy: minutely=%s, hourly=%s, daily=%s, weekly=%s, monthly=%s, yearly=%s", options.minutelyParams[0], options.hourlyParams[0], options.dailyParams[0], options.weeklyParams[0], options.monthlyParams[0], options.yearlyParams[0])

	electedBackups := make([]MaterializedBackup, 0)
	electedBackups = appendElectedForTag("", "0", electedBackups)
	electedBackups = appendElectedForTag("minutely", options.minutelyParams[0], electedBackups)
	electedBackups = appendElectedForTag("hourly", options.hourlyParams[0], electedBackups)
	electedBackups = appendElectedForTag("daily", options.dailyParams[0], electedBackups)
	electedBackups = appendElectedForTag("weekly", options.weeklyParams[0], electedBackups)
	electedBackups = appendElectedForTag("monthly", options.monthlyParams[0], electedBackups)
	electedBackups = appendElectedForTag("yearly", options.yearlyParams[0], electedBackups)
	logrus.Infof("%d backups elected for deletion", len(electedBackups))

	for _, backup := range electedBackups {
		logrus.Debugf("Deleting backup '%s'...", backup.ID)
		res, err := setStatusMaterializedBackup(backup.ID, "deleting")
		ra, _ := res.RowsAffected()
		if err != nil {
			logrus.Errorf("Couldn't set status of backup '%s' to 'deleting'. Skipping backup deletion. err=%s", backup.ID, err)
			retentionBackupsDeleteErrorCounter.Inc()
		} else if ra != 1 {
			logrus.Errorf("Strange number of affected rows while setting status of backup '%s' to 'deleting'. Skipping backup deletion. rowsAffected=%d", backup.ID, ra)
			retentionBackupsDeleteErrorCounter.Inc()
		} else {
			performBackupDelete(backup.ID)
			//give some breath to backed webhook
			// time.Sleep(1000 * time.Millisecond)
		}
	}

	elapsed := time.Now().Sub(start)
	logrus.Infof("Retention management task done. elapsed=%s", elapsed)
	avoidRetentionLock.Unlock()
}

func performBackupDelete(backupID string) {
	err := deleteWebhookBackup(backupID)
	if err != nil {
		logrus.Warnf("Could not delete backup '%s' using webhook. err=%s", backupID, err)
		_, err0 := setStatusMaterializedBackup(backupID, "delete-error")
		if err0 != nil {
			logrus.Warnf("Could not set backup %s status to 'delete-error'. err=%s", backupID, err0)
		}
		retentionBackupsDeleteErrorCounter.Inc()
	} else {
		logrus.Infof("Backup '%s' deleted successfuly", backupID)
		_, err0 := setStatusMaterializedBackup(backupID, "deleted")
		if err0 != nil {
			logrus.Warnf("Could not set backup %s status to 'deleted'. err=%s", backupID, err0)
			retentionBackupsDeleteErrorCounter.Inc()
		} else {
			retentionBackupsDeleteSuccessCounter.Inc()
		}
	}
}

func retryDeleteErrors() {
	logrus.Debugf("Retrying webhook delete for backups with 'delete-error' tag")
	backups, err := getMaterializedBackups(10, "", "delete-error", true)
	if err != nil {
		logrus.Errorf("Couldn't query backups tagged as 'delete-error'. err=%s", err)
	} else if len(backups) > 0 {
		logrus.Infof("%d backups tagged with 'backup-error' randomly gotten (limiting to 10). retrying to delete them on webhook", len(backups))
		for _, backup := range backups {
			retentionBackupsRetriesCounter.Inc()
			performBackupDelete(backup.ID)
		}
	} else {
		logrus.Debugf("No backups tagged with 'delete-error'")
	}
}

func appendElectedForTag(tag string, retentionCount string, appendTo []MaterializedBackup) []MaterializedBackup {
	ret, err0 := strconv.Atoi(retentionCount)
	if err0 != nil {
		logrus.Errorf("%s: Invalid retention parameter: err=%s", tag, err0)
		return appendTo
	}
	mbackups, err := getExclusiveTagAvailableMaterializedBackups(tag, ret, 10)
	if err != nil {
		logrus.Errorf("%s: Error querying backups for deletion. err=%s", tag, err)
		return appendTo
	}
	logrus.Debugf("%s: %d backups elected for deletion (limited to 10)", tag, len(mbackups))
	return append(appendTo, mbackups...)
}
