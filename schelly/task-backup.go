package main

import (
	"time"
	"github.com/Sirupsen/logrus"
)

func runBackupTask() {
	start := time.Now()
	logrus.Info("Triggering a new backup task...")

	elapsed := time.Now().Sub(start)	
	logrus.Infof("Backup task done. elapsed=%s", elapsed)
}

