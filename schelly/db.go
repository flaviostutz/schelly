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
	"github.com/prometheus/client_golang/prometheus"
)

var metricsSQLCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_sql_total",
	Help: "Total database statements executed",
}, []string{
	"status",
})

//MaterializedBackup backup record
type MaterializedBackup struct {
	ID         string
	DataID     string
	StartTime  time.Time
	EndTime    time.Time
	Status     string
	CustomData string
	SizeMB     float64
	Reference  int
	Minutely   int
	Hourly     int
	Daily      int
	Weekly     int
	Monthly    int
	Yearly     int
}

var db = &sql.DB{}

func initDB() error {
	prometheus.MustRegister(metricsSQLCounter)

	db0, err := sql.Open("sqlite3", fmt.Sprintf("%s/sqlite.db", options.dataDir))
	if err != nil {
		return err
	}
	statement, err1 := db0.Prepare("CREATE TABLE IF NOT EXISTS materialized_backup (id TEXT NOT NULL, data_id TEXT NOT NULL, status TEXT NOT NULL, start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NOT NULL DEFAULT `2000-01-01`, custom_data TEXT NOT NULL DEFAULT ``, size REAL, minutely INTEGER NOT NULL DEFAULT 0, hourly INTEGER NOT NULL DEFAULT 0, daily INTEGER NOT NULL DEFAULT 0, weekly INTEGER NOT NULL DEFAULT 0, monthly INTEGER NOT NULL DEFAULT 0, yearly INTEGER NOT NULL DEFAULT 0, reference INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(`id`))")
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

func createMaterializedBackup(backupID string, dataID string, status string, startDate time.Time, endDate time.Time, customData string, size float64) (string, error) {
	stmt, err1 := db.Prepare("INSERT INTO materialized_backup (id, data_id, status, start_time, end_time, custom_data, size) values(?,?,?,?,?,?,?)")
	if err1 != nil {
		return "", err1
	}
	_, err2 := stmt.Exec(backupID, dataID, status, startDate, endDate, customData, size)
	if err2 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return "", err2
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	// rows, _ := db.Query("SELECT id,  FROM backup_tasks")
	return backupID, nil
}

func getMaterializedBackup(backupID string) (MaterializedBackup, error) {
	rows, err1 := db.Query("SELECT id,data_id,status,start_time,end_time,custom_data,size,reference,minutely,hourly,daily,weekly,monthly,yearly FROM materialized_backup WHERE id='" + backupID + "'")
	if err1 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return MaterializedBackup{}, err1
	}
	defer rows.Close()

	for rows.Next() {
		backup := MaterializedBackup{}
		err2 := rows.Scan(&backup.ID, &backup.DataID, &backup.Status, &backup.StartTime, &backup.EndTime, &backup.CustomData, &backup.SizeMB, &backup.Reference, &backup.Minutely, &backup.Hourly, &backup.Daily, &backup.Weekly, &backup.Monthly, &backup.Yearly)
		if err2 != nil {
			metricsSQLCounter.WithLabelValues("error").Inc()
			return MaterializedBackup{}, err2
		} else {
			metricsSQLCounter.WithLabelValues("success").Inc()
			return backup, nil
		}
	}
	err := rows.Err()
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return MaterializedBackup{}, err
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
		return MaterializedBackup{}, fmt.Errorf("Backup id %s not found", backupID)
	}
}

func getMaterializedBackups(limit int, tag string, status string, randomOrder bool) ([]MaterializedBackup, error) {
	where := ""
	if tag != "" || status != "" {
		where = where + " WHERE "
		if tag != "" {
			where = where + " " + tag + "=1"
		}
		if status != "" {
			if tag != "" {
				where = where + " AND "
			}
			where = where + " status='" + status + "'"
		}
	}
	orderBy := "start_time DESC"
	if randomOrder {
		orderBy = "RANDOM()"
	}
	q := "SELECT id,data_id,status,start_time,end_time,custom_data,size,reference,minutely,hourly,daily,weekly,monthly,yearly FROM materialized_backup " + where + " ORDER BY " + orderBy
	if limit != 0 {
		q = q + fmt.Sprintf(" LIMIT %d", limit)
	}
	logrus.Debugf("query=%s", q)
	rows, err1 := db.Query(q)
	if err1 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err1
	}
	defer rows.Close()

	var backups = make([]MaterializedBackup, 0)
	for rows.Next() {
		backup := MaterializedBackup{}
		err2 := rows.Scan(&backup.ID, &backup.DataID, &backup.Status, &backup.StartTime, &backup.EndTime, &backup.CustomData, &backup.SizeMB, &backup.Reference, &backup.Minutely, &backup.Hourly, &backup.Daily, &backup.Weekly, &backup.Monthly, &backup.Yearly)
		if err2 != nil {
			metricsSQLCounter.WithLabelValues("error").Inc()
			return []MaterializedBackup{}, err2
		} else {
			backups = append(backups, backup)
		}
	}
	err := rows.Err()
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	return backups, nil
}

func getExclusiveTagAvailableMaterializedBackups(tag string, skipNewestCount int, limit int) ([]MaterializedBackup, error) {
	whereTags := ""
	tags := []string{"minutely", "hourly", "daily", "weekly", "monthly", "yearly"}
	if tag != "" {
		for _, t := range tags {
			if t == tag {
				whereTags = whereTags + t + "=1"
			} else if whereTags != "" {
				whereTags = whereTags + " AND " + t + "=0"
			}
		}
	} else {
		for _, t := range tags {
			if whereTags != "" {
				whereTags = whereTags + " AND "
			}
			whereTags = whereTags + t + "=0"
		}
	}

	q := fmt.Sprintf("SELECT id,data_id,status,start_time,end_time,custom_data,reference,minutely,hourly,daily,weekly,monthly,yearly FROM materialized_backup WHERE %s AND status='available' ORDER BY start_time DESC LIMIT %d OFFSET %d", whereTags, limit, skipNewestCount)
	logrus.Debugf("getExclusiveTags query=%s", q)
	rows, err1 := db.Query(q)
	if err1 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err1
	}
	defer rows.Close()

	var backups = make([]MaterializedBackup, 0)
	for rows.Next() {
		backup := MaterializedBackup{}
		err2 := rows.Scan(&backup.ID, &backup.DataID, &backup.Status, &backup.StartTime, &backup.EndTime, &backup.CustomData, &backup.Reference, &backup.Minutely, &backup.Hourly, &backup.Daily, &backup.Weekly, &backup.Monthly, &backup.Yearly)
		if err2 != nil {
			metricsSQLCounter.WithLabelValues("error").Inc()
			return []MaterializedBackup{}, err2
		} else {
			backups = append(backups, backup)
		}
	}
	err := rows.Err()
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	return backups, nil
}

func clearTagsAndReferenceMaterializedBackup(tx *sql.Tx) (sql.Result, error) {
	stmt, err := db.Prepare("UPDATE materialized_backup SET reference=0, minutely=0, hourly=0, daily=0, weekly=0, monthly=0, yearly=0;")
	if err != nil {
		return nil, err
	}
	res, err0 := tx.Stmt(stmt).Exec()
	if err0 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
	}
	return res, err0
}

func setAllTagsMaterializedBackup(tx *sql.Tx, backupID string) (sql.Result, error) {
	stmt, err := db.Prepare("UPDATE materialized_backup SET minutely=1, hourly=1, daily=1, weekly=1, monthly=1, yearly=1 WHERE id=?;")
	if err != nil {
		return nil, err
	}
	res, err0 := tx.Stmt(stmt).Exec(backupID)
	if err0 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
	}
	return res, err0
}

func markReferencesMinutelyMaterializedBackup(tx *sql.Tx, secondReference string) (sql.Result, error) {
	sql := `UPDATE materialized_backup set reference=1, minutely=1
											WHERE id IN (
												SELECT y.id AS id FROM 
												(SELECT id, strftime('%Y-%m-%dT%H:%M:0.000', start_time) AS timeref, MIN(ABS(strftime('%S', start_time)-` + secondReference + `)) AS refdiff
													FROM materialized_backup p
													GROUP BY strftime('%Y-%m-%dT%H:%M:0.000', start_time)) y
											)`
	logrus.Debugf("sql=%s", sql)
	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	res, err0 := tx.Stmt(stmt).Exec()
	if err0 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
	}
	return res, err0
}

func setStatusMaterializedBackup(backupID string, status string) (sql.Result, error) {
	sql := `UPDATE materialized_backup SET status=? WHERE id=?`
	stmt, err := db.Prepare(sql)
	logrus.Infof("%s %s %s", sql, backupID, status)
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return nil, err
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
	}
	return stmt.Exec(status, backupID)
}

func markTagMaterializedBackup(tx *sql.Tx, tag string, previousTag string, groupByPattern string, diffPattern string, ref string) (sql.Result, error) {
	sql := `UPDATE materialized_backup set ` + tag + `=1
								WHERE id IN (
									SELECT y.id AS id FROM 
									(SELECT id, strftime('` + groupByPattern + `', start_time) AS timeref, MIN(ABS(strftime('` + diffPattern + `', start_time)-` + ref + `)) AS refdiff
										FROM materialized_backup p
										WHERE reference=1 AND ` + previousTag + `=1
										GROUP BY strftime('` + groupByPattern + `', start_time)) y
								)`
	logrus.Debugf("sql=%s", sql)
	stmt, err := db.Prepare(sql)
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return nil, err
	}
	res, err0 := tx.Stmt(stmt).Exec()
	if err0 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
	}
	return res, err0
}

func getTags(backup MaterializedBackup) []string {
	t := make([]string, 0)
	if backup.Reference == 1 {
		t = append(t, "reference")
	}
	if backup.Minutely == 1 {
		t = append(t, "minutely")
	}
	if backup.Hourly == 1 {
		t = append(t, "hourly")
	}
	if backup.Daily == 1 {
		t = append(t, "daily")
	}
	if backup.Weekly == 1 {
		t = append(t, "weekly")
	}
	if backup.Monthly == 1 {
		t = append(t, "monthly")
	}
	if backup.Yearly == 1 {
		t = append(t, "yearly")
	}
	return t
}
