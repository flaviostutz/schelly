package schelly

import (
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

//BackupSpec bs
type BackupSpec struct {
	ID                         string     `json:"id,omitempty" bson:"id"`
	Name                       string     `json:"name,omitempty" bson:"name"`
	Enabled                    bool       `json:"enabled,omitempty" bson:"enabled"`
	WorkflowName               string     `json:"workflowName,omitempty" bson:"workflowName"`
	WorkflowVersion            string     `json:"workflowVersion,omitempty" bson:"workflowVersion"`
	CheckWarningSeconds        int        `json:"checkWarningSeconds,omitempty" bson:"checkWarningSeconds"`
	FromDate                   *time.Time `json:"fromDate,omitempty" bson:"fromDate"`
	ToDate                     *time.Time `json:"toDate,omitempty" bson:"toDate"`
	LastUpdate                 time.Time  `json:"lastUpdate,omitempty" bson:"lastUpdate"`
	RetentionMinutelyCount     int32      `json:"retentionMinutelyCount,omitempty"`
	RetentionMinutelyReference string     `json:"retentionMinutelyReference,omitempty"`
	RetentionHourlyCount       int32      `json:"retentionHourlyCount,omitempty"`
	RetentionHourlyReference   string     `json:"retentionHourlyReference,omitempty"`
	RetentionDailyCount        int32      `json:"retentionDailyCount,omitempty"`
	RetentionWeeklyReference   string     `json:"retentionWeeklyReference,omitempty"`
	RetentionMonthlyCount      int32      `json:"retentionMonthlyCount,omitempty"`
	RetentionYearlyReference   string     `json:"retentionYearlyReference,omitempty"`
}

func createBackupSpec(bs BackupSpec) error {
	stmt, err1 := db.Prepare(`INSERT INTO backup_spec (
								id, name, enabled, workflow_name, workflow_version, 
								check_warning_seconds, from_date, to_date, last_update, 
								retention_minutely, retention_hourly, retention_daily, retention_weekly, 
								retention_monthly, retention_yearly
							) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec(bs.ID, bs.Name, bs.Enabled, bs.WorkflowName, bs.WorkflowVersion,
		bs.CheckWarningSeconds, bs.FromDate, bs.ToDate, bs.LastUpdate,
		bs.RetentionMinutely, bs.RetentionHourly, bs.RetentionDaily, bs.RetentionWeekly,
		bs.RetentionMonthly, bs.RetentionYearly)
	if err2 != nil {
		return err2
	}
	return nil
}

func updateBackupSpec(bs BackupSpec) error {
	stmt, err1 := db.Prepare(`UPDATE backup_spec SET
								name=?, enabled=?, workflow_name=?, workflow_version=?, 
								check_warning_seconds=?, from_date=?, to_date=?, last_update=?, 
								retention_minutely=?, retention_hourly=?, retention_daily=?, retention_weekly=?, 
								retention_monthly=?, retention_yearly=? 
							  WHERE id='` + bs.ID + `'`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec(bs.Name, bs.Enabled, bs.WorkflowName, bs.WorkflowVersion,
		bs.CheckWarningSeconds, bs.FromDate, bs.ToDate, bs.LastUpdate,
		bs.RetentionMinutely, bs.RetentionHourly, bs.RetentionDaily, bs.RetentionWeekly,
		bs.RetentionMonthly, bs.RetentionYearly)
	if err2 != nil {
		return err2
	}
	return nil
}

func getBackupSpec(id string) (BackupSpec, error) {
	rows, err1 := db.Query(`SELECT 
			id, name, enabled, workflow_name, workflow_version, 
			check_warning_seconds, from_date, to_date, last_update, 
			retention_minutely, retention_hourly, retention_daily, retention_weekly, 
			retention_monthly, retention_yearly
			FROM backup_spec WHERE id='` + id + `'`)
	if err1 != nil {
		return BackupSpec{}, err1
	}
	defer rows.Close()

	for rows.Next() {
		b := BackupSpec{}
		err2 := rows.Scan(&b.ID, &b.Name, &b.Enabled, &b.WorkflowName, &b.WorkflowVersion,
			&b.CheckWarningSeconds, &b.FromDate, &b.ToDate, &b.LastUpdate,
			&b.RetentionMinutely, &b.RetentionHourly, &b.RetentionDaily, &b.RetentionWeekly,
			&b.RetentionMonthly, &b.RetentionYearly)
		if err2 != nil {
			return BackupSpec{}, err2
		} else {
			return b, nil
		}
	}
	err := rows.Err()
	if err != nil {
		return BackupSpec{}, err
	} else {
		return BackupSpec{}, fmt.Errorf("Backup spec id %s not found", id)
	}
}

func listBackupSpecs(status string) ([]BackupSpec, error) {
	where := ""
	if status != "" {
		where = "WHERE status='" + status + "'"
	}
	q := `SELECT 
			id, name, enabled, workflow_name, workflow_version, 
			check_warning_seconds, from_date, to_date, last_update, 
			retention_minutely, retention_hourly, retention_daily, retention_weekly, 
			retention_monthly, retention_yearly
		FROM backup_spec ` + where + ` ORDER BY name`

	logrus.Debugf("query=%s", q)
	rows, err1 := db.Query(q)
	if err1 != nil {
		return []BackupSpec{}, err1
	}
	defer rows.Close()

	var backups = make([]BackupSpec, 0)
	for rows.Next() {
		b := BackupSpec{}
		err2 := rows.Scan(&b.ID, &b.Name, &b.Enabled, &b.WorkflowName, &b.WorkflowVersion,
			&b.CheckWarningSeconds, &b.FromDate, &b.ToDate, &b.LastUpdate,
			&b.RetentionMinutely, &b.RetentionHourly, &b.RetentionDaily, &b.RetentionWeekly,
			&b.RetentionMonthly, &b.RetentionYearly)
		if err2 != nil {
			return []BackupSpec{}, err2
		} else {
			backups = append(backups, b)
		}
	}
	err := rows.Err()
	if err != nil {
		return []BackupSpec{}, err
	}
	return backups, nil
}

func deleteBackupSpec(id string) error {
	stmt, err1 := db.Prepare(`DELETE backup_spec 
							  WHERE id='` + id + `'`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec()
	if err2 != nil {
		return err2
	}
	return nil
}
