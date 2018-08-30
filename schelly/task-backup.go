package main

import (
	"time"

	"github.com/Sirupsen/logrus"
)

func runBackupTask() {
	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP TASK")
	logrus.Info("")

	logrus.Debug("Checking if there is another running backup")

	backupID, backupStatus, backupDate, err := getCurrentTaskStatus()
	if err != nil {
		logrus.Warnf("Couldn't get current task id from file. err=%s", err)
	} else {
		if backupStatus == "running" {
			logrus.Infof("Another backup task %s is still running (%s). Skipping backup.", backupID, time.Now().Sub(backupDate))
			return
		}
	}

	logrus.Debugf("Invoking POST '%s' so that a new backup will be created", options.webhookURL)
	startPostTime := time.Now()

	resp, err1 := createWebhookBackup()
	if err1 != nil {
		logrus.Warnf("Couldn't invoke webhook for backup creation. err=%s", err1)
	} else {
		if resp.Status == "available" {
			logrus.Infof("Backup executed successfuly and is already available. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
			mid, err1 := createMaterializedBackup(resp.ID, "", startPostTime, time.Now(), "")
			if err1 != nil {
				logrus.Errorf("Couldn't create materialized backup on database. err=%s", err1)
			} else {
				logrus.Infof("Materialized backup reference saved to database successfuly. id=%s", mid)
				setCurrentTaskStatus(resp.ID, resp.Status, startPostTime)
			}
		} else if resp.Status == "running" {
			logrus.Infof("Backup invoked successfuly but it is still running in background (not available yet). Starting to check for completion from time to time. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
		} else {
			logrus.Warnf("Backup invoked but an unrecognized status was returned. Won't track it. id=%s; status=%s message=%s", resp.ID, resp.Status, resp.Message)
		}
		setCurrentTaskStatus(resp.ID, resp.Status, startPostTime)
	}

	elapsed := time.Now().Sub(start)
	logrus.Infof("Backup task done. elapsed=%s", elapsed)
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
				logrus.Infof("Backup %s finished on backend server. status=%s", backupID, resp.Status)
				mid, err1 := createMaterializedBackup(resp.ID, "", backupDate, time.Now(), "")
				if err1 != nil {
					logrus.Errorf("Couldn't create materialized backup on database. err=%s", err1)
				} else {
					logrus.Infof("Materialized backup reference saved to database successfuly. id=%s", mid)
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
