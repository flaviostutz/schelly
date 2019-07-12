package schelly

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var (
	metricsInitialized = false
)

//METRICS
var backupLastSizeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "schelly_backup_last_size_mbytes",
	Help: "Last successful backup size in bytes",
}, []string{
	"backup",
})

var backupLastTimeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "schelly_backup_last_time_seconds",
	Help: "Last successful backup time",
}, []string{
	"backup",
})

var backupTasksCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_backup_tasks_total",
	Help: "Total backup tasks triggered",
}, []string{
	"backup",
	"status",
})

var backupTriggerCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_backup_trigger_total",
	Help: "Total backups triggered",
}, []string{
	"backup",
	"status",
})

var backupMaterializedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_backup_materialized_total",
	Help: "Total backups materialized",
}, []string{
	"backup",
	"status",
})

var backupTagCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_backup_tag_total",
	Help: "Total backups that were tagged",
}, []string{
	"backup",
	"status",
})

var overallBackupWarnCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_backup_warn_total",
	Help: "Total overall backup warnings",
}, []string{
	"backup",
	"status",
})

type BackupTask struct {
	Spec              BackupSpec
	runningBackupTask bool
}

func NewBackupTask(Spec BackupSpec) BackupTask {
	if !metricsInitialized {
		prometheus.MustRegister(backupLastSizeGauge)
		prometheus.MustRegister(backupLastTimeGauge)
		prometheus.MustRegister(backupTasksCounter)
		prometheus.MustRegister(backupMaterializedCounter)
		prometheus.MustRegister(backupTagCounter)
		prometheus.MustRegister(overallBackupWarnCounter)
		metricsInitialized = true
	}
	return BackupTask{Spec, false}
}

func (b *BackupTask) RunBackupTask() {
	if b.runningBackupTask {
		logrus.Debug("runBackupTask already running. skipping new task creation")
		backupTasksCounter.WithLabelValues(b.Spec.Name, "skipped").Inc()
		overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "warning").Inc()
		return
	} else {
		b.runningBackupTask = true
		backupTasksCounter.WithLabelValues(b.Spec.Name, "run").Inc()
	}

	start := time.Now()

	for b.runningBackupTask {
		_, err := b.triggerNewBackup()
		elapsed := time.Now().Sub(start)
		if err != nil {
			if elapsed.Seconds() < opt.BackupTimeout {
				logrus.Errorf("Error triggering backup. Retrying until grace time in 5 seconds. err=%s", err)
				time.Sleep(5 * time.Second)
				backupTriggerCounter.WithLabelValues(b.Spec.Name, "retry").Inc()
				overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "warning").Inc()
			} else {
				logrus.Errorf("Error triggering backup. Grace time reached. Won't retry anymore. err=%s", err)
				b.runningBackupTask = false
				backupTriggerCounter.WithLabelValues(b.Spec.Name, "error").Inc()
				overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "error").Inc()
			}
		} else {
			logrus.Infof("Backup task done. elapsed=%s", elapsed)
			b.runningBackupTask = false
			backupTriggerCounter.WithLabelValues(b.Spec.Name, "success").Inc()
		}
	}
}

func (b *BackupTask) triggerNewBackup() (ResponseWebhook, error) {
	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP TASK")

	logrus.Debugf("Checking if there is another backup running. name=%s", b.Spec.Name)

	backupID, backupStatus, backupDate, err := getCurrentTaskStatus(b.Spec.Name)
	if err != nil {
		return ResponseWebhook{}, fmt.Errorf("Couldn't get current task id from file. err=%s", err)
	}

	if backupStatus == "running" {
		logrus.Infof("Another backup task %s is still running (%s). name=%s.", backupID, b.Spec.Name)
		overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "warning").Inc()
		return ResponseWebhook{}, nil
	}

	logrus.Debugf("Invoking POST '%s' so that a new backup will be created", options.ConductorAPIURL)
	startPostTime := time.Now()

	resp, err1 := createWebhookBackup()
	if err1 != nil {
		overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "error").Inc()
		return resp, fmt.Errorf("Couldn't invoke webhook for backup creation. err=%s", err1)
	}

	if resp.Status == "running" {
		logrus.Infof("Backup invoked successfuly. Starting to check for completion from time to time. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
		setCurrentTaskStatus(resp.ID, resp.Status, startPostTime)
	} else {
		logrus.Warnf("Backup invoked but an unrecognized status was returned. Won't track it. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
		overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "error").Inc()
		setCurrentTaskStatus(resp.ID, resp.Status, startPostTime)
	}

	elapsed := time.Now().Sub(start)
	logrus.Debugf("Backup triggering done. elapsed=%s", elapsed)
	return resp, nil
}

func tagAllBackups(backupSpec BackupSpec) error {
	logrus.Debugf("Tagging backups")

	//begin transaction
	logrus.Debug("Begining db transaction")
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Error begining db transaction. err=%s", err)
	}

	//check last backup
	logrus.Debug("Checking for backups available")
	backups, err1 := getMaterializedBackups(backupSpec.Name, 1, "", "available", false)
	if err1 != nil {
		tx.Rollback()
		return fmt.Errorf("Error getting last backup. err=%s", err)
	} else if len(backups) == 0 {
		logrus.Warnf("No backups found. Skipping tagging.")
		tx.Rollback()
		return nil
	}
	lastBackup := backups[0]

	logrus.Debug("Clearing all backup tags")
	res, err0 := clearTagsAndReferenceMaterializedBackup(tx)
	if err0 != nil {
		tx.Rollback()
		return fmt.Errorf("Error clearing tags. err=%s", err0)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//minutely
	logrus.Debugf("Marking reference + minutely tags")
	res, err = markReferencesMinutelyMaterializedBackup(tx, backupSpec.Name, backupSpec.MinutelyParams()[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking reference+minutely tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//hourly
	logrus.Debugf("Marking hourly tags")
	res, err = markTagMaterializedBackup(tx, "hourly", "minutely", "%Y-%m-%dT%H:0:0.000", "%M", backupSpec.Name, backupSpec.HourlyParams()[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking hourly tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//daily
	logrus.Debugf("Marking daily tags")
	res, err = markTagMaterializedBackup(tx, "daily", "hourly", "%Y-%m-%w-%dT0:0:0.000", "%H", backupSpec.Name, backupSpec.DailyParams()[1])
	if err != nil {
		tx.Rollback()
		backupTagCounter.WithLabelValues(backupSpec.Name, "error").Inc()
		return fmt.Errorf("Error marking daily tags. err=%s", err)
	}
	tc, _ := res.RowsAffected()
	logrus.Debugf("%d rows affected", tc)

	//weekly
	logrus.Debugf("Marking weekly tags")
	res, err = markTagMaterializedBackup(tx, "weekly", "daily", "%Y-%m-%W-0T0:0:0.000", "%w", backupSpec.Name, backupSpec.WeeklyParams()[1])
	if err != nil {
		tx.Rollback()
		backupTagCounter.WithLabelValues(backupSpec.Name, "error").Inc()
		return fmt.Errorf("Error marking weekly tags. err=%s", err)
	}
	tc, _ = res.RowsAffected()
	logrus.Debugf("%d rows affected", tc)

	//monthly
	logrus.Debugf("Marking monthly tags")
	ref := backupSpec.MonthlyParams()[1]
	if ref == "L" {
		ref = "31"
	}
	res, err = markTagMaterializedBackup(tx, "monthly", "daily", "%Y-%m-0T0:0:0.000", "%d", ref)
	if err != nil {
		tx.Rollback()
		backupTagCounter.WithLabelValues(backupSpec.Name, "error").Inc()
		return fmt.Errorf("Error marking monthly tags. err=%s", err)
	}
	tc, _ = res.RowsAffected()
	logrus.Debugf("%d rows affected", tc)

	//yearly
	logrus.Debugf("Marking yearly tags")
	res, err = markTagMaterializedBackup(tx, "yearly", "monthly", "%Y-0-0T0:0:0.000", "%m", backupSpec.Name, backupSpec.YearlyParams()[1])
	if err != nil {
		tx.Rollback()
		backupTagCounter.WithLabelValues(backupSpec.Name, "error").Inc()
		return fmt.Errorf("Error marking yearly tags. err=%s", err)
	}
	tc, _ = res.RowsAffected()
	logrus.Debugf("%d rows affected", tc)

	logrus.Debug("Tagging last backup with all tags")
	res, err = setAllTagsMaterializedBackup(tx, lastBackup.ID)
	if err != nil {
		tx.Rollback()
		backupTagCounter.WithLabelValues(backupSpec.Name, "error").Inc()
		return fmt.Errorf("Error tagging last backup. err=%s", err)
	}
	tc, _ = res.RowsAffected()
	logrus.Debugf("%d rows affected", tc)

	logrus.Debug("Commiting transaction")
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		backupTagCounter.WithLabelValues(backupSpec.Name, "error").Inc()
		return fmt.Errorf("Error commiting transation. err=%s", err)
	}
	backupTagCounter.WithLabelValues(backupSpec.Name, "success").Inc()
	return nil
}

func (b *BackupTask) CheckBackupTask() {
	logrus.Debugf("checkBackupTask %s", b.Spec.Name)
	backupID, backupStatus, backupDate, err := getCurrentTaskStatus(backupSpec.Name)
	if err != nil {
		logrus.Debugf("Couldn't load task status file. Ignoring. err=%s", err)
		overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "warning").Inc()
	}
	if backupStatus == "running" {
		resp, err := getWebhookBackupInfo(backupID)
		if err != nil {
			logrus.Warnf("Couldn't get backup %s info from webhook. err=%s", backupID, err)
			b.checkGraceTime()
		} else {
			if resp.Status != backupStatus {
				logrus.Infof("Backup %s finish detected on backend server. status=%s", backupID, resp.Status)
				//avoid doing retention until the newly created backup is tagged to avoid it to be elected for removal (because it will have no tags)
				avoidRetentionLock.Lock()
				mid, err1 := createMaterializedBackup(resp.ID, resp.DataID, resp.Status, backupDate, time.Now(), resp.Message, resp.SizeMB)
				if err1 != nil {
					logrus.Errorf("Couldn't create materialized backup on database. err=%s", err1)
					avoidRetentionLock.Unlock()
					overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "error").Inc()
				} else {
					logrus.Debugf("Materialized backup reference saved to database successfuly. id=%s", mid)
					setCurrentTaskStatus(backupID, resp.Status, backupDate)
					backupMaterializedCounter.WithLabelValues(b.Spec.Name, "success").Inc()
					if resp.SizeMB != 0 {
						backupLastSizeGauge.Set(float64(resp.SizeMB))
					}
					backupLastTimeGauge.Set(float64(time.Now().Sub(backupDate).Seconds()))
					err = b.tagAllBackups()
					if err != nil {
						overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "error").Inc()
					}
					avoidRetentionLock.Unlock()
				}
			}
			b.checkGraceTime()
		}
	}
}

func (b *BackupTask) checkGraceTime() {
	logrus.Debugf("Verifying if current backup is taking too long. If it exceeds graceTime, cancel it on the backend server")
	backupID, backupStatus, backupDate, err := getCurrentTaskStatus()
	if backupStatus == "running" {
		if time.Now().Sub(backupDate).Seconds() > options.BackupTimeout {
			logrus.Warnf("Grace time for backup %s exceeded. Cancelling backup...", backupID)
			err = deleteWebhookBackup(backupID)
			if err != nil {
				logrus.Errorf("Couldn't cancel running backup %s task on webhook. err=%s", backupID, err)
				backupMaterializedCounter.WithLabelValues(b.Spec.Name, "error").Inc()
				setCurrentTaskStatus(backupID, "error", backupDate)
			} else {
				logrus.Infof("Running backup task %s cancelled on webhook successfuly", backupID)
				backupMaterializedCounter.WithLabelValues(b.Spec.Name, "cancelled").Inc()
				setCurrentTaskStatus(backupID, "cancelled", backupDate)
			}
			overallBackupWarnCounter.WithLabelValues(b.Spec.Name, "error").Inc()
		}
	}
}

func mu(a ...interface{}) []interface{} {
	return a
}
