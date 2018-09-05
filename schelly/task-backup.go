package main

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
)

var runningBackupTask = false

func runBackupTask() {
	if runningBackupTask {
		logrus.Debug("runBackupTask already running. skipping new task creation")
		return
	} else {
		runningBackupTask = true
	}

	start := time.Now()

	for runningBackupTask {
		_, err := triggerNewBackup()
		elapsed := time.Now().Sub(start)
		if err != nil {
			if elapsed.Seconds() < options.graceTimeSeconds {
				logrus.Errorf("Error triggering backup. Retrying until grace time in 5 seconds. err=%s", err)
				time.Sleep(5 * time.Second)
			} else {
				logrus.Errorf("Error triggering backup. Won't retry anymore. err=%s", err)
			}
		} else {
			logrus.Infof("Backup task done. elapsed=%s", elapsed)
		}
	}
	runningBackupTask = false
}

func triggerNewBackup() (ResponseWebhook, error) {
	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP TASK")

	logrus.Debug("Checking if there is another backup running")

	backupID, backupStatus, backupDate, err := getCurrentTaskStatus()
	if err != nil {
		logrus.Warnf("Couldn't get current task id from file. err=%s", err)
	} else {
		if backupStatus == "running" {
			logrus.Infof("Another backup task %s is still running (%s). Skipping backup.", backupID, time.Now().Sub(backupDate))
			return ResponseWebhook{}, nil
		}
	}

	logrus.Debugf("Invoking POST '%s' so that a new backup will be created", options.webhookURL)
	startPostTime := time.Now()

	resp, err1 := createWebhookBackup()
	if err1 != nil {
		return resp, fmt.Errorf("Couldn't invoke webhook for backup creation. err=%s", err1)
	} else {
		if resp.Status == "available" {
			logrus.Infof("Backup executed successfuly and is already available. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
			mid, err1 := createMaterializedBackup(resp.ID, resp.Status, startPostTime, time.Now(), resp.Message)
			if err1 != nil {
				return resp, fmt.Errorf("Couldn't create materialized backup on database. err=%s", err1)
			} else {
				logrus.Debugf("Materialized backup reference saved to database successfuly. id=%s", mid)
				setCurrentTaskStatus(resp.ID, resp.Status, startPostTime)
				err1 = tagAllBackups()
				if err1 != nil {
					return resp, fmt.Errorf("Cannot tag backups. err=%s", err1)
				} else {
					logrus.Debug("Backups tagging run successfuly")
				}
			}
		} else if resp.Status == "running" {
			logrus.Infof("Backup invoked successfuly but it is still running in background (not available yet). Starting to check for completion from time to time. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
		} else {
			logrus.Warnf("Backup invoked but an unrecognized status was returned. Won't track it. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
		}
		setCurrentTaskStatus(resp.ID, resp.Status, startPostTime)
	}

	elapsed := time.Now().Sub(start)
	logrus.Debugf("Backup triggering done. elapsed=%s", elapsed)
	return resp, nil
}

func tagAllBackups() error {
	logrus.Debugf("Tagging backups")

	//begin transaction
	logrus.Debug("Begining db transaction")
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("Error begining db transaction. err=%s", err)
	}

	//check last backup
	logrus.Debug("Checking for backups available")
	backups, err1 := getMaterializedBackups(1, "", "available", false)
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
	res, err = markReferencesMinutelyMaterializedBackup(tx, options.minutelyParams[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking reference+minutely tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//hourly
	logrus.Debugf("Marking hourly tags")
	res, err = markTagMaterializedBackup(tx, "hourly", "minutely", "%Y-%m-%dT%H:0:0.000", "%M", options.hourlyParams[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking hourly tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//weekly
	// logrus.Debugf("Marking weekly tags")
	// err = markTagMaterializedBackup(tx *sql.Tx, "weekly", "%Y-%m-%dT%H:0:0.000", "%M", options.hourlyParams[1])
	// if err != nil {
	// 	logrus.Errorf("Error marking hourly tags. err=%s", err)
	// 	tx.Rollback()
	// 	return
	// }

	//daily
	logrus.Debugf("Marking daily tags")
	res, err = markTagMaterializedBackup(tx, "daily", "hourly", "%Y-%m-%w-%dT0:0:0.000", "%H", options.dailyParams[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking daily tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//weekly
	logrus.Debugf("Marking weekly tags")
	res, err = markTagMaterializedBackup(tx, "weekly", "daily", "%Y-%m-%W-0T0:0:0.000", "%w", options.weeklyParams[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking weekly tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//monthly
	logrus.Debugf("Marking monthly tags")
	ref := options.monthlyParams[1]
	if ref == "L" {
		ref = "31"
	}
	res, err = markTagMaterializedBackup(tx, "monthly", "daily", "%Y-%m-0T0:0:0.000", "%d", ref)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking monthly tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	//yearly
	logrus.Debugf("Marking yearly tags")
	res, err = markTagMaterializedBackup(tx, "yearly", "monthly", "%Y-0-0T0:0:0.000", "%m", options.yearlyParams[1])
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error marking yearly tags. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	logrus.Debug("Tagging last backup with all tags")
	res, err = setAllTagsMaterializedBackup(tx, lastBackup.ID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error tagging last backup. err=%s", err)
	}
	logrus.Debugf("%d rows affected", mu(res.RowsAffected())[0])

	logrus.Debug("Commiting transaction")
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("Error commiting transation. err=%s", err)
	}
	return nil
}

func checkBackupTask() {
	logrus.Debug("checkBackupTask")
	backupID, backupStatus, backupDate, err := getCurrentTaskStatus()
	if err != nil {
		logrus.Debugf("Couldn't load task status file. Ignoring. err=%s", err)
	}
	if backupStatus == "running" {
		resp, err := getWebhookBackupInfo(backupID)
		if err != nil {
			logrus.Warnf("Couldn't get backup %s info from webhook. err=%s", backupID, err)
			checkGraceTime()
		} else {
			if resp.Status != backupStatus {
				logrus.Infof("Backup %s finish detected on backend server. status=%s", backupID, resp.Status)
				mid, err1 := createMaterializedBackup(resp.ID, resp.Status, backupDate, time.Now(), resp.Message)
				if err1 != nil {
					logrus.Errorf("Couldn't create materialized backup on database. err=%s", err1)
				} else {
					logrus.Debugf("Materialized backup reference saved to database successfuly. id=%s", mid)
					setCurrentTaskStatus(backupID, resp.Status, backupDate)
				}
			}
			checkGraceTime()
		}
	}
}

func checkGraceTime() {
	logrus.Debugf("Verifying if current backup is taking too long. If it exceeds graceTime, cancel it on the backend server")
	backupID, backupStatus, backupDate, err := getCurrentTaskStatus()
	if backupStatus == "running" {
		if time.Now().Sub(backupDate).Seconds() > options.graceTimeSeconds {
			logrus.Warnf("Grace time for backup %s exceeded. Cancelling backup...", backupID)
			err = deleteWebhookBackup(backupID)
			if err != nil {
				logrus.Errorf("Couldn't cancel running backup %s task on webhook. err=%s", backupID, err)
				setCurrentTaskStatus(backupID, "error", backupDate)
			} else {
				logrus.Infof("Running backup task %s cancelled on webhook successfuly", backupID)
				setCurrentTaskStatus(backupID, "canceled", backupDate)
			}
		}
	}
}

func mu(a ...interface{}) []interface{} {
	return a
}
