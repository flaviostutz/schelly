package main

import (
	"time"

	"github.com/Sirupsen/logrus"
)

func runRetentionTask() {
	start := time.Now()
	logrus.Info("Triggering retention management task...")

	elapsed := time.Now().Sub(start)
	logrus.Infof("Retention management task done. elapsed=%s", elapsed)
}
