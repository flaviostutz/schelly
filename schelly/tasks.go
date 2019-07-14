package schelly

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

var (
	opt                    Options
	db                     *sql.DB
	scheduledRoutineHashes = make(map[string]*cron.Cron)
)

//Options command line options used to run Schelly
type Options struct {
	ConductorAPIURL string
	DataDir         string
}

func InitAll(opt0 Options) error {
	opt = opt0

	InitConductor()
	db0, err := InitDB()
	if err != nil {
		return err
	}
	db = db0

	InitTaskBackup()
	InitTaskRetention()

	err1 := prepareTimers()
	if err1 != nil {
		return err1
	}

	h := NewHTTPServer()
	err2 := h.Start()
	if err2 != nil {
		logrus.Errorf("Error starting server. err=%s", err2)
	}
	return nil
}

func prepareTimers() error {
	logrus.Debugf("Refreshing timers according to active schedules")

	//activate go routines for backup spec that weren't activated yet
	a := 1
	enabledBackupSpecs, err := listBackupSpecs(&a)
	if err != nil {
		return err
	}
	for _, bs := range enabledBackupSpecs {
		isScheduled := false
		activeRoutineHash := fmt.Sprintf("%s|%s)", bs.Name, bs.BackupCronString)
		for hashRoutine := range scheduledRoutineHashes {
			if activeRoutineHash == hashRoutine {
				isScheduled = true
				break
			}
		}
		if !isScheduled {
			err := launchBackupRoutine(bs.Name)
			if err != nil {
				return err
			}
		}
	}

	//remove go routines that are not currently active
	for hashRoutine, cronJob := range scheduledRoutineHashes {
		isActive := false
		for _, bs := range enabledBackupSpecs {
			activeRoutineHash := fmt.Sprintf("%s|%s)", bs.Name, bs.BackupCronString)
			if hashRoutine == activeRoutineHash {
				isActive = true
				break
			}
		}
		if !isActive {
			logrus.Infof("Schedule %s: Stopping timer", hashRoutine)
			cronJob.Stop()
			delete(scheduledRoutineHashes, hashRoutine)
		}
	}

	return nil
}

func launchBackupRoutine(backupName string) error {
	bs1, err := getBackupSpec(backupName)
	if err != nil {
		return fmt.Errorf("Couldn't load backup spec %s. err=%s", backupName, err)
	}

	c := cron.New()
	logrus.Infof("Creating timer for backup %s. cron=%s", backupName, bs1.BackupCronString)
	c.AddFunc(*bs1.BackupCronString, func() {
		logrus.Debugf("Timer triggered for backup %s", backupName)

		bs, err := getBackupSpec(backupName)
		if err != nil {
			logrus.Errorf("Couldn't load backup spec %s. err=%s", backupName, err)
			return
		}

		isBefore := false
		if bs.ToDate == nil || time.Now().Before(*bs.ToDate) {
			isBefore = true
		}
		isAfter := false
		if bs.FromDate == nil || time.Now().After(*bs.FromDate) {
			isAfter = true
		}

		if isBefore && isAfter {

			wid, err := triggerNewBackup(backupName)
			if err != nil {
				logrus.Warnf("Error launching backup workflow for backup %s. err=%s", backupName, err)
				backupTriggerCounter.WithLabelValues(backupName, "error").Inc()
				overallBackupWarnCounter.WithLabelValues(backupName, "warning").Inc()
			} else {
				logrus.Infof("Backup launched. workflowId=%s", wid)
				backupTriggerCounter.WithLabelValues(backupName, "success").Inc()
			}

			checkBackupWorkflow(backupName)

			RunRetentionTask(backupName)

			checkWorkflowBackupRemove(backupName)

		} else {
			logrus.Debugf("Backup %s is enabled, but not within activation date", backupName)
		}

	})
	routineHash := fmt.Sprintf("%s|%s)", backupName, *bs1.BackupCronString)
	scheduledRoutineHashes[routineHash] = c
	go c.Start()
	return nil
}

// func launchBackupRoutine() {
// 	c := cron.New()
// 	c.AddFunc(options.BackupCron, func() { schelly.RunBackupTask() })
// 	c.AddFunc("@every 5s", func() { schelly.CheckBackupTask() })
// 	c.AddFunc(options.RetentionCron, func() { schelly.RunRetentionTask() })
// 	c.AddFunc("@every 1d", func() { schelly.RetryDeleteErrors() })
// 	go c.Start()
// }
