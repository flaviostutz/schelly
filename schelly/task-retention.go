package main

import (
	"time"

	"github.com/Sirupsen/logrus"
)

func runRetentionTask() {
	start := time.Now()
	logrus.Info("")
	logrus.Info(">>>> BACKUP RETENTION MANAGEMENT")

	backups, err := getAllMaterializedBackups()
	if err != nil {
		logrus.Errorf("Couldn't get materialized_backups. err=%s", err)
		return
	}
	logrus.Debugf("Found %d backup references in local database", len(backups))

	//mark last backup with all tags

	elapsed := time.Now().Sub(start)
	logrus.Infof("Retention management task done. elapsed=%s", elapsed)
}
