package schelly

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var metricsSQLCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_sql_total",
	Help: "Total database statements executed",
}, []string{
	"status",
})

//MaterializedBackup backup record
type MaterializedBackup struct {
	ID                      string
	DataID                  string
	Status                  string
	BackupName              string
	StartTime               time.Time
	EndTime                 time.Time
	SizeMB                  float64
	RunningDeleteWorkflowID *string
	Reference               int
	Minutely                int
	Hourly                  int
	Daily                   int
	Weekly                  int
	Monthly                 int
	Yearly                  int
}

func createMaterializedBackup(id string, backupName string, dataID *string, status string, startDate time.Time, endDate time.Time, size *float64) error {
	if id == "" {
		return fmt.Errorf("'id' must be defined")
	}
	stmt, err1 := db.Prepare("INSERT INTO materialized_backup (id, backup_name, data_id, status, start_time, end_time, size) values(?,?,?,?,?,?,?)")
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec(id, backupName, dataID, status, startDate, endDate, size)
	if err2 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return err2
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	// rows, _ := db.Query("SELECT id,  FROM backup_tasks")
	return nil
}

func getMaterializedBackup(id string) (MaterializedBackup, error) {
	rows, err1 := db.Query("SELECT id,data_id,status,start_time,end_time,running_delete_workflow,size,reference,minutely,hourly,daily,weekly,monthly,yearly FROM materialized_backup WHERE id='" + id + "'")
	if err1 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return MaterializedBackup{}, err1
	}
	defer rows.Close()

	for rows.Next() {
		backup := MaterializedBackup{}
		err2 := rows.Scan(&backup.ID, &backup.DataID, &backup.Status, &backup.StartTime, &backup.EndTime, &backup.RunningDeleteWorkflowID, &backup.SizeMB, &backup.Reference, &backup.Minutely, &backup.Hourly, &backup.Daily, &backup.Weekly, &backup.Monthly, &backup.Yearly)
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
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	return MaterializedBackup{}, fmt.Errorf("Backup id %s not found", id)
}

func getMaterializedBackups(backupName string, limit int, tag string, status string, randomOrder bool) ([]MaterializedBackup, error) {
	where := fmt.Sprintf(" WHERE backup_name='%s'", backupName)
	if tag != "" || status != "" {
		if tag != "" {
			where = where + " AND " + tag + "=1"
		}
		if status != "" {
			where = where + " AND status='" + status + "'"
		}
	}
	orderBy := "start_time DESC"
	if randomOrder {
		orderBy = "RANDOM()"
	}
	q := "SELECT id,data_id,status,backup_name,start_time,end_time,running_delete_workflow,size,reference,minutely,hourly,daily,weekly,monthly,yearly FROM materialized_backup " + where + " ORDER BY " + orderBy
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

	var materializeds = make([]MaterializedBackup, 0)
	for rows.Next() {
		m := MaterializedBackup{}
		err2 := rows.Scan(&m.ID, &m.DataID, &m.Status, &m.BackupName, &m.StartTime, &m.EndTime, &m.RunningDeleteWorkflowID, &m.SizeMB, &m.Reference, &m.Minutely, &m.Hourly, &m.Daily, &m.Weekly, &m.Monthly, &m.Yearly)
		if err2 != nil {
			metricsSQLCounter.WithLabelValues("error").Inc()
			return []MaterializedBackup{}, err2
		} else {
			materializeds = append(materializeds, m)
		}
	}
	err := rows.Err()
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	return materializeds, nil
}

func getExclusiveTagAvailableMaterializedBackups(backupName string, tag string, skipNewestCount int, limit int) ([]MaterializedBackup, error) {
	whereTags := fmt.Sprintf(" WHERE backup_name='%s'", backupName)
	tags := []string{"minutely", "hourly", "daily", "weekly", "monthly", "yearly"}
	if tag != "" {
		for _, t := range tags {
			if t == tag {
				whereTags = whereTags + " AND " + t + "=1"
			} else if whereTags != "" {
				whereTags = whereTags + " AND " + t + "=0"
			}
		}
	} else {
		for _, t := range tags {
			whereTags = whereTags + " AND " + t + "=0"
		}
	}

	q := fmt.Sprintf("SELECT id,data_id,status,backup_name,start_time,end_time,running_delete_workflow,reference,minutely,hourly,daily,weekly,monthly,yearly FROM materialized_backup WHERE %s AND status='available' ORDER BY start_time DESC LIMIT %d OFFSET %d", whereTags, limit, skipNewestCount)
	logrus.Debugf("getExclusiveTags query=%s", q)
	rows, err1 := db.Query(q)
	if err1 != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err1
	}
	defer rows.Close()

	var mbs = make([]MaterializedBackup, 0)
	for rows.Next() {
		m := MaterializedBackup{}
		err2 := rows.Scan(&m.ID, &m.DataID, &m.Status, &m.BackupName, &m.StartTime, &m.EndTime, &m.RunningDeleteWorkflowID, &m.Reference, &m.Minutely, &m.Hourly, &m.Daily, &m.Weekly, &m.Monthly, &m.Yearly)
		if err2 != nil {
			metricsSQLCounter.WithLabelValues("error").Inc()
			return []MaterializedBackup{}, err2
		} else {
			mbs = append(mbs, m)
		}
	}
	err := rows.Err()
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return []MaterializedBackup{}, err
	}
	metricsSQLCounter.WithLabelValues("success").Inc()
	return mbs, nil
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

func markReferencesMinutelyMaterializedBackup(tx *sql.Tx, backupName string, secondReference string) (sql.Result, error) {
	sql := `UPDATE materialized_backup set reference=1, minutely=1
											WHERE id IN (
												SELECT y.id AS id FROM 
												(SELECT id, strftime('%Y-%m-%dT%H:%M:0.000', start_time) AS timeref, MIN(ABS(strftime('%S', start_time)-` + secondReference + `)) AS refdiff
													FROM materialized_backup p
													WHERE backup_name='` + backupName + `'
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

func setStatusMaterializedBackup(materializedID string, status string) (sql.Result, error) {
	sql := `UPDATE materialized_backup SET status=? WHERE id=?`
	stmt, err := db.Prepare(sql)
	logrus.Infof("%s %s %s", sql, materializedID, status)
	if err != nil {
		metricsSQLCounter.WithLabelValues("error").Inc()
		return nil, err
	} else {
		metricsSQLCounter.WithLabelValues("success").Inc()
	}
	return stmt.Exec(status, materializedID)
}

func markTagMaterializedBackup(tx *sql.Tx, backupName string, tag string, previousTag string, groupByPattern string, diffPattern string, ref string) (sql.Result, error) {
	sql := `UPDATE materialized_backup set ` + tag + `=1
								WHERE id IN (
									SELECT y.id AS id FROM 
									(SELECT id, strftime('` + groupByPattern + `', start_time) AS timeref, MIN(ABS(strftime('` + diffPattern + `', start_time)-` + ref + `)) AS refdiff
										FROM materialized_backup p
										WHERE backup_name='` + backupName + `' AND reference=1 AND ` + previousTag + `=1
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
