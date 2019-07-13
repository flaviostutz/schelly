package schelly

import (
	"fmt"
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
	backupName string
	running    bool
}

func NewRetentionTask(backupName string) BackupTask {
	if !retentionInitialized {
		prometheus.MustRegister(retentionTasksCounter)
		prometheus.MustRegister(retentionBackupsDeleteCounter)
		prometheus.MustRegister(retentionBackupsRetriesCounter)
		retentionInitialized = true
	}
	return BackupTask{backupName, false}
}

func (r *RetentionTask) RunRetentionTask() {
	if r.running {
		logrus.Debugf("runRetentionTask for %s already running", r.backupName)
		return
	}
	r.running = true

	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP RETENTION MANAGEMENT")
	retentionTasksCounter.Inc()

	bs, err := getBackupSpec(r.backupName)
	if err != nil {

	}

	avoidRetentionLock.Lock()
	defer avoidRetentionLock.Unlock()

	tagAllBackups(r.backupName)

	logrus.Debugf("Retention policy: minutely=%s, hourly=%s, daily=%s, weekly=%s, monthly=%s, yearly=%s", bs.MinutelyParams()[0], bs.HourlyParams()[0], bs.DailyParams()[0], bs.WeeklyParams()[0], bs.MonthlyParams()[0], bs.YearlyParams()[0])

	electedBackups := make([]MaterializedBackup, 0)
	electedBackups = appendElectedForTag(r.backupName, "", "0", electedBackups)
	electedBackups = appendElectedForTag(r.backupName, "minutely", bs.MinutelyParams()[0], electedBackups)
	electedBackups = appendElectedForTag(r.backupName, "hourly", bs.HourlyParams()[0], electedBackups)
	electedBackups = appendElectedForTag(r.backupName, "daily", bs.DailyParams()[0], electedBackups)
	electedBackups = appendElectedForTag(r.backupName, "weekly", bs.WeeklyParams()[0], electedBackups)
	electedBackups = appendElectedForTag(r.backupName, "monthly", bs.MonthlyParams()[0], electedBackups)
	electedBackups = appendElectedForTag(r.backupName, "yearly", bs.YearlyParams()[0], electedBackups)
	logrus.Infof("%d backups elected for deletion", len(electedBackups))

	for _, backup := range electedBackups {
		logrus.Debugf("Deleting backup '%s'...", backup.ID)
		res, err := setStatusMaterializedBackup(backup.ID, "deleting")
		ra, _ := res.RowsAffected()
		if err != nil {
			logrus.Errorf("Couldn't set status of backup '%s' to 'deleting'. Skipping backup deletion. err=%s", backup.ID, err)
			retentionBackupsDeleteCounter.WithLabelValues("error").Inc()
			continue
		} else if ra != 1 {
			logrus.Errorf("Strange number of affected rows while setting status of backup '%s' to 'deleting'. Skipping backup deletion. rowsAffected=%d", backup.ID, ra)
			retentionBackupsDeleteCounter.WithLabelValues("error").Inc()
			continue
		}
		err2 := triggerBackupDelete(backup.ID)
		if err2 != nil {
			logrus.Warnf("Couldn't trigger backup delete for materialized backup %s. err=%s", backup.ID, err2)
		}
		//give some breath to backed webhook
		// time.Sleep(1000 * time.Millisecond)
	}

	elapsed := time.Now().Sub(start)
	logrus.Infof("Retention management task done. elapsed=%s", elapsed)
	r.running = false
}

func triggerBackupDelete(materializedID string) error {
	mb, err := getMaterializedBackup(materializedID)
	if err != nil {
		return fmt.Errorf("Couldn't load materized backup %s", materializedID)
	}

	if mb.Status != "completed" {
		return fmt.Errorf("Materialized backup %s cannot be deleted because its status is not 'completed'. status=%s", mb.ID, mb.Status)
	}

	if mb.RunningDeleteWorkflowID != nil {
		return fmt.Errorf("Materialized backup %s cannot be deleted because it already has a runningWorkflowID set", mb.ID)
	}

	workflowID, err1 := launchRemoveBackupWorkflow(mb.DataID)
	if err1 != nil {
		overallBackupWarnCounter.WithLabelValues(mb.BackupName, "error").Inc()
		logrus.Warnf("Couldn't invoke Conductor workflow for backup removal. err=%s", err1)
	}
	logrus.Infof("Backup %s delete workflow launched successfuly for dataID %s. workflowID=%s", mb.BackupName, mb.DataID, workflowID)

	_, err2 := setStatusMaterializedBackup(materializedID, "deleting")
	if err2 != nil {
		return fmt.Errorf("Couldn't update status of materialized backup %s to 'deleting'. err=%s", mb.ID, err2)
	}

	return nil
}

func checkWorkflowBackupRemove(backupName string) {
	logrus.Debugf("checkWorkflowBackupRemove backupName=%s", backupName)

	mbs, err := getMaterializedBackups(backupName, 1, "", "deleting", false)
	if err != nil {
		logrus.Warnf("Couldn't load materializeds for backup %s", backupName)
		overallBackupWarnCounter.WithLabelValues("none", "error").Inc()
	}
	if len(mbs) == 0 {
		logrus.Debugf("No materialized backups pending delete for backup %s", backupName)
		return
	}

	mb := mbs[0]
	if mb.RunningDeleteWorkflowID == nil {
		logrus.Errorf("Materialized backup %s has no running delete workflow set but status is 'deleting'", mb.ID)
		overallBackupWarnCounter.WithLabelValues("none", "error").Inc()
		return
	}

	wf, err0 := getWorkflowInstance(*mb.RunningDeleteWorkflowID)
	if err0 != nil {
		logrus.Debugf("Couldn't get workflow instance %s. err=%s", *mb.RunningDeleteWorkflowID, err0)
		overallBackupWarnCounter.WithLabelValues(backupName, "error").Inc()
		return
	}

	if wf.status == "running" {
		logrus.Debugf("Workflow %s for removing materialized backup is still running", *mb.RunningDeleteWorkflowID)
		// if time.Now().Sub(wf.startTime).Seconds() > float64(bs.TimeoutSeconds) {
		// logrus.Warnf("Materialized backup removal for backup %s timeout. Check conductor workflow", mb.BackupName)
		// }
		return
	}

	logrus.Infof("Conductor workflow %s for backup deletion of %s has finished. status=%s", wf.workflowID, backupName, wf.status)
	//avoid doing retention until the newly created backup is tagged to avoid it to be elected for removal (because it will have no tags)
	avoidRetentionLock.Lock()
	defer avoidRetentionLock.Unlock()

	if wf.status != "completed" {
		logrus.Warnf("Workflow %s has finished but there is some error. status=%s. backupName=%s. dataId=%s", wf.workflowID, wf.status, mb.BackupName, mb.DataID)
		retentionBackupsDeleteCounter.WithLabelValues(backupName, wf.status).Inc()
		_, err2 := setStatusMaterializedBackup(mb.ID, "delete-error")
		if err2 != nil {
			logrus.Errorf("Couldn't set materialized backup status. err=%s", err2)
		}
		return
	}

	logrus.Warnf("Workflow %s has completed and backup was removed. dataId=%s. backupName=%s", wf.workflowID, mb.DataID, mb.BackupName)
	retentionBackupsDeleteCounter.WithLabelValues(backupName, wf.status).Inc()

	_, err2 := setStatusMaterializedBackup(mb.ID, "deleted")
	if err2 != nil {
		logrus.Errorf("Couldn't set materialized backup status. err=%s", err2)
		overallBackupWarnCounter.WithLabelValues(backupName, "error").Inc()
		return
	}
}

func appendElectedForTag(backupName string, tag string, retentionCount string, appendTo []MaterializedBackup) []MaterializedBackup {
	ret, err0 := strconv.Atoi(retentionCount)
	if err0 != nil {
		logrus.Errorf("%s: Invalid retention parameter: err=%s", tag, err0)
		return appendTo
	}
	mbackups, err := getExclusiveTagAvailableMaterializedBackups(backupName, tag, ret, 10)
	if err != nil {
		logrus.Errorf("%s: Error querying backups for deletion. err=%s", tag, err)
		return appendTo
	}
	logrus.Debugf("%s: %d backups elected for deletion (limited to 10)", tag, len(mbackups))
	return append(appendTo, mbackups...)
}
