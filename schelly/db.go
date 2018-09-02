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
	ID          string
	StartTime   time.Time
	EndTime     time.Time
	Tags        string
	Status      string
	CustomData  string
	IsReference int
}

var db = &sql.DB{}

func initDB() error {
	db0, err := sql.Open("sqlite3", fmt.Sprintf("%s/sqlite.db", options.dataDir))
	if err != nil {
		return err
	}
	statement, err1 := db0.Prepare("CREATE TABLE IF NOT EXISTS materialized_backup (id TEXT NOT NULL, status TEXT NOT NULL, start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NOT NULL DEFAULT `2000-01-01`, custom_data TEXT NOT NULL DEFAULT ``, tags TEXT NOT NULL DEFAULT ``, is_reference INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(`id`))")
	if err1 != nil {
		return err1
	}
	_, err1 = statement.Exec()
	if err1 != nil {
		return err1
	}

	os.MkdirAll(options.dataDir, os.ModePerm)

	db = db0
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

func createMaterializedBackup(backupID string, status string, startDate time.Time, endDate time.Time, customData string) (string, error) {
	stmt, err1 := db.Prepare("INSERT INTO materialized_backup (id, status, start_time, end_time, custom_data) values(?,?,?,?,?)")
	if err1 != nil {
		return "", err1
	}
	_, err2 := stmt.Exec(backupID, status, startDate, endDate, customData)
	if err2 != nil {
		return "", err2
	}
	// rows, _ := db.Query("SELECT id,  FROM backup_tasks")
	return backupID, nil
}

func getMaterializedBackup(backupID string) (MaterializedBackup, error) {
	rows, err1 := db.Query("SELECT id,tags,status,start_time,end_time,custom_data,is_reference FROM materialized_backup WHERE id='" + backupID + "'")
	if err1 != nil {
		return MaterializedBackup{}, err1
	}
	defer rows.Close()

	for rows.Next() {
		backup := MaterializedBackup{}
		err2 := rows.Scan(&backup.ID, &backup.Tags, &backup.Status, &backup.StartTime, &backup.EndTime, &backup.CustomData, &backup.IsReference)
		if err2 != nil {
			return MaterializedBackup{}, err2
		} else {
			return backup, nil
		}
	}
	err := rows.Err()
	if err != nil {
		return MaterializedBackup{}, err
	} else {
		return MaterializedBackup{}, fmt.Errorf("Backup id %s not found", backupID)
	}
}

func getAllMaterializedBackups(limit int) ([]MaterializedBackup, error) {
	q := "SELECT id,tags,status,start_time,end_time,custom_data,is_reference FROM materialized_backup ORDER BY start_time DESC")
	if limit != 0 {
		q = q + fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err1 := db.Query(q)
	if err1 != nil {
		return []MaterializedBackup{}, err1
	}
	defer rows.Close()

	var backups = make([]MaterializedBackup, 0)
	for rows.Next() {
		backup := MaterializedBackup{}
		err2 := rows.Scan(&backup.ID, &backup.Tags, &backup.Status, &backup.StartTime, &backup.EndTime, &backup.CustomData, &backup.IsReference)
		if err2 != nil {
			return []MaterializedBackup{}, err2
		} else {
			backups = append(backups, backup)
		}
	}
	err := rows.Err()
	if err != nil {
		return []MaterializedBackup{}, err
	}
	return backups, nil
}

// func setTagsMaterializedBackup(backupID string, tags string) error {
// 	// logrus.Infof("setTagsMaterializedBackup id=%s tags=%s", backupID, tags)
// 	stmt, err1 := db.Prepare("UPDATE materialized_backup SET tags=? WHERE id=?")
// 	if err1 != nil {
// 		return err1
// 	}
// 	_, err2 := stmt.Exec(tags, backupID)
// 	if err2 != nil {
// 		return err2
// 	}

// 	return nil
// }

// func removeTagMaterializedBackup(backupID string, tag string) error {
// 	backup, err := getMaterializedBackup(backupID)
// 	if err != nil {
// 		return err
// 	} else if backup.ID == "" {
// 		return fmt.Errorf("Backup %s not found in database", backupID)
// 	}

// 	if !tagFound(backup.Tags, tag) {
// 		return fmt.Errorf("Couldn't find tag '%s' on backup %s. tags=%s", tag, backup.ID, backup.Tags)
// 	}

// 	ts := strings.Split(backup.Tags, ",")
// 	newTags := ""
// 	for _, t := range ts {
// 		if t != tag {
// 			if newTags != "" {
// 				newTags = newTags + "," + t
// 			} else {
// 				newTags = t
// 			}
// 		}
// 	}
// 	return setTagsMaterializedBackup(backupID, newTags)
// }

// func addTagMaterializedBackup(backupID string, tag string) error {
// 	// logrus.Infof("addTagMaterializedBackup id=%s tag=%s", backupID, tag)
// 	backup, err := getMaterializedBackup(backupID)
// 	if err != nil {
// 		return err
// 	} else if backup.ID == "" {
// 		return fmt.Errorf("Backup %s not found in database", backupID)
// 	}

// 	if tagFound(backup.Tags, tag) {
// 		return fmt.Errorf("Couldn't find tag '%s' on backup %s. tags=%s", tag, backup.ID, backup.Tags)
// 	}

// 	newTags := tag
// 	if backup.Tags != "" {
// 		newTags = backup.Tags + "," + tag
// 	}
// 	return setTagsMaterializedBackup(backupID, newTags)
// }

// func markAsReferenceMaterializedBackup(backupID string, isReference bool) error {
// 	stmt, err1 := db.Prepare("UPDATE materialized_backup SET is_reference=? WHERE id=?")
// 	if err1 != nil {
// 		return err1
// 	}
// 	ref := 0
// 	if isReference {
// 		ref = 1
// 	}
// 	_, err2 := stmt.Exec(ref, backupID)
// 	if err2 != nil {
// 		return err2
// 	}
// 	return nil
// }

// func isReferenceMaterializedBackup(backupID string) (bool, error) {
// 	backup, err := getMaterializedBackup(backupID)
// 	if err != nil {
// 		return false, err
// 	} else if backup.ID == "" {
// 		return false, fmt.Errorf("Backup %s not found in database", backupID)
// 	} else {
// 		return backup.IsReference == 1, nil
// 	}
// }

func tagFound(tags string, tag string) bool {
	found := false
	ts := strings.Split(","+tags+",", ",")
	for _, t := range ts {
		if t == tag {
			found = true
		}
	}
	return found
}
