package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3"
)

type MaterializedBackup struct {
	ID        string
	StartTime time.Time
	EndTime   time.Time
	tags      string
}

func initDB() error {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s/sqlite.db", options.dataDir))
	if err != nil {
		return err
	}
	statement, err1 := db.Prepare("CREATE TABLE IF NOT EXISTS materialized_backup (id TEXT, status TEXT, start_time TIMESTAMP, end_time TIMESTAMP)")
	if err1 != nil {
		return err1
	}
	_, err1 = statement.Exec()
	if err1 != nil {
		return err1
	}

	os.MkdirAll(options.dataDir, os.ModePerm)

	logrus.Debug("Database initialized")
	return nil
}

func setCurrentTaskStatus(id string, status string, date time.Time) error {
	ft := date.Format(time.RFC3339)
	return ioutil.WriteFile(fmt.Sprintf("%s/backup-task", options.dataDir), []byte(fmt.Sprintf("%s|%s|%s", id, status, ft)), 0644)
}

//returns backupId, backupStatus, time, error
func getCurrentTaskStatus() (string, string, time.Time, error) {
	b, err := ioutil.ReadFile(fmt.Sprintf("%s/backup-task", options.dataDir))
	line := string(b)
	if err != nil {
		return "", "", time.Now(), err
	}
	params := strings.Split(line, "|")
	if len(params) != 3 {
		return "", "", time.Now(), fmt.Errorf("Invalid params found in /data/backup-task: %s", line)
	}
	t, err1 := time.Parse(time.RFC3339, params[2])
	if err1 != nil {
		return "", "", time.Now(), err1
	}
	return params[0], params[1], t, nil
}

func createMaterializedBackup(backupID string, tags string, startDate time.Time, endDate time.Time, customData string) (string, error) {
	// rows, _ := db.Query("SELECT id,  FROM backup_tasks")
	// stmt, err := db.Prepare("INSERT INTO userinfo(username, departname, created) values(?,?,?)")
	// res, err := stmt.Exec("astaxie", "研发部门", "2012-12-09")
	return backupID, nil
}
