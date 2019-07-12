package schelly

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

//METRICS
var retentionTasksCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_retention_tasks_total",
	Help: "Total retention tasks triggered",
})

var retentionBackupsDeleteCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_retention_backup_delete_total",
	Help: "Total retention backups deleted",
}, []string{
	"status",
})

var retentionBackupsRetriesCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_retention_backup_delete_retries_total",
	Help: "Total retention backup delete retries",
})

var retentionInitialized = false
var avoidRetentionLock = &sync.Mutex{}

type RetentionTask struct {
	Spec    BackupSpec
	running bool
}

func NewRetentionTask(Spec BackupSpec) BackupTask {
	if !retentionInitialized {
		prometheus.MustRegister(retentionTasksCounter)
		prometheus.MustRegister(retentionBackupsDeleteCounter)
		prometheus.MustRegister(retentionBackupsRetriesCounter)
		retentionInitialized = true
	}
	return BackupTask{Spec, false}
}

func (r *RetentionTask) RunRetentionTask() {
	if r.running {
		logrus.Debugf("runRetentionTask for %s already running. skipping new task creation", r.Spec.Name)
		return
	} else {
		r.running = true
	}
	r.triggerRetentionTask()
	r.running = false
}

func (r *RetentionTask) triggerRetentionTask() {
	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP RETENTION MANAGEMENT")
	retentionTasksCounter.Inc()

	avoidRetentionLock.Lock()

	tagAllBackups(r.Spec)

	logrus.Debugf("Retention policy: minutely=%s, hourly=%s, daily=%s, weekly=%s, monthly=%s, yearly=%s", r.Spec.MinutelyParams()[0], r.Spec.HourlyParams()[0], r.Spec.DailyParams()[0], r.Spec.WeeklyParams()[0], r.Spec.MonthlyParams()[0], r.Spec.YearlyParams()[0])

	electedBackups := make([]MaterializedBackup, 0)
	electedBackups = r.appendElectedForTag("", "0", electedBackups)
	electedBackups = r.appendElectedForTag("minutely", r.Spec.MinutelyParams()[0], electedBackups)
	electedBackups = r.appendElectedForTag("hourly", r.Spec.HourlyParams()[0], electedBackups)
	electedBackups = r.appendElectedForTag("daily", r.Spec.DailyParams()[0], electedBackups)
	electedBackups = r.appendElectedForTag("weekly", r.Spec.WeeklyParams()[0], electedBackups)
	electedBackups = r.appendElectedForTag("monthly", r.Spec.MonthlyParams()[0], electedBackups)
	electedBackups = r.appendElectedForTag("yearly", r.Spec.YearlyParams()[0], electedBackups)
	logrus.Infof("%d backups elected for deletion", len(electedBackups))

	for _, backup := range electedBackups {
		logrus.Debugf("Deleting backup '%s'...", backup.ID)
		res, err := setStatusMaterializedBackup(backup.ID, "deleting")
		ra, _ := res.RowsAffected()
		if err != nil {
			logrus.Errorf("Couldn't set status of backup '%s' to 'deleting'. Skipping backup deletion. err=%s", backup.ID, err)
			retentionBackupsDeleteCounter.WithLabelValues("error").Inc()
		} else if ra != 1 {
			logrus.Errorf("Strange number of affected rows while setting status of backup '%s' to 'deleting'. Skipping backup deletion. rowsAffected=%d", backup.ID, ra)
			retentionBackupsDeleteCounter.WithLabelValues("error").Inc()
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
		retentionBackupsDeleteCounter.WithLabelValues("error").Inc()
	} else {
		logrus.Infof("Backup '%s' deleted successfuly", backupID)
		_, err0 := setStatusMaterializedBackup(backupID, "deleted")
		if err0 != nil {
			logrus.Warnf("Could not set backup %s status to 'deleted'. err=%s", backupID, err0)
			retentionBackupsDeleteCounter.WithLabelValues("error").Inc()
		} else {
			retentionBackupsDeleteCounter.WithLabelValues("success").Inc()
		}
	}
}

func (r *RetentionTask) RetryDeleteErrors() {
	logrus.Debugf("Retrying webhook delete for backups with 'delete-error' tag")
	backups, err := getMaterializedBackups(r.Spec.Name, 10, "", "delete-error", true)
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

func (r *RetentionTask) appendElectedForTag(tag string, retentionCount string, appendTo []MaterializedBackup) []MaterializedBackup {
	ret, err0 := strconv.Atoi(retentionCount)
	if err0 != nil {
		logrus.Errorf("%s: Invalid retention parameter: err=%s", tag, err0)
		return appendTo
	}
	mbackups, err := getExclusiveTagAvailableMaterializedBackups(r.Spec.Name, tag, ret, 10)
	if err != nil {
		logrus.Errorf("%s: Error querying backups for deletion. err=%s", tag, err)
		return appendTo
	}
	logrus.Debugf("%s: %d backups elected for deletion (limited to 10)", tag, len(mbackups))
	return append(appendTo, mbackups...)
}
