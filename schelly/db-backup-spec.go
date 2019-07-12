package schelly

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

//BackupSpec bs
type BackupSpec struct {
	Name                string     `json:"name,omitempty" bson:"name"`
	Enabled             bool       `json:"enabled,omitempty" bson:"enabled"`
	WorkflowName        string     `json:"workflowName,omitempty" bson:"workflowName"`
	WorkflowVersion     string     `json:"workflowVersion,omitempty" bson:"workflowVersion"`
	CheckWarningSeconds int        `json:"checkWarningSeconds,omitempty" bson:"checkWarningSeconds"`
	FromDate            *time.Time `json:"fromDate,omitempty" bson:"fromDate"`
	ToDate              *time.Time `json:"toDate,omitempty" bson:"toDate"`
	LastUpdate          time.Time  `json:"lastUpdate,omitempty" bson:"lastUpdate"`
	RetentionMinutely   string     `json:"retentionMinutely,omitempty"`
	RetentionHourly     string     `json:"retentionHourly,omitempty"`
	RetentionDaily      string     `json:"retentionDaily,omitempty"`
	RetentionWeekly     string     `json:"retentionWeekly,omitempty"`
	RetentionMonthly    string     `json:"retentionMonthly,omitempty"`
	RetentionYearly     string     `json:"retentionYearly,omitempty"`
}

func createBackupSpec(bs BackupSpec) error {
	stmt, err1 := db.Prepare(`INSERT INTO backup_spec (
								name, enabled, workflow_name, workflow_version, 
								check_warning_seconds, from_date, to_date, last_update, 
								retention_minutely, retention_hourly, retention_daily, retention_weekly, 
								retention_monthly, retention_yearly
							) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
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

func updateBackupSpec(bs BackupSpec) error {
	stmt, err1 := db.Prepare(`UPDATE backup_spec SET
								name=?, enabled=?, workflow_name=?, workflow_version=?, 
								check_warning_seconds=?, from_date=?, to_date=?, last_update=?, 
								retention_minutely=?, retention_hourly=?, retention_daily=?, retention_weekly=?, 
								retention_monthly=?, retention_yearly=? 
							  WHERE name='` + bs.Name + `'`)
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

func getBackupSpec(backupName string) (BackupSpec, error) {
	rows, err1 := db.Query(`SELECT 
			name, enabled, workflow_name, workflow_version, 
			check_warning_seconds, from_date, to_date, last_update, 
			retention_minutely, retention_hourly, retention_daily, retention_weekly, 
			retention_monthly, retention_yearly
			FROM backup_spec WHERE name='` + backupName + `'`)
	if err1 != nil {
		return BackupSpec{}, err1
	}
	defer rows.Close()

	for rows.Next() {
		b := BackupSpec{}
		err2 := rows.Scan(&b.Name, &b.Enabled, &b.WorkflowName, &b.WorkflowVersion,
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
		return BackupSpec{}, fmt.Errorf("Backup spec name %s not found", backupName)
	}
}

func listBackupSpecs(status string) ([]BackupSpec, error) {
	where := ""
	if status != "" {
		where = "WHERE status='" + status + "'"
	}
	q := `SELECT 
			name, enabled, workflow_name, workflow_version, 
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
		err2 := rows.Scan(&b.Name, &b.Enabled, &b.WorkflowName, &b.WorkflowVersion,
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

func deleteBackupSpec(backupName string) error {
	stmt, err1 := db.Prepare(`DELETE backup_spec 
							  WHERE name='` + backupName + `'`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec()
	if err2 != nil {
		return err2
	}
	return nil
}

func retentionParams(config string, lastReference string) []string {
	if config == "" {
		return []string{"0", lastReference}
	}
	params := strings.Split(config, "@")
	if len(params) == 1 {
		params = append(params, "L")
	}
	if params[1] == "" {
		params[1] = "L"
	}
	if params[1] == "L" {
		params[1] = lastReference
	}
	return params
}

func (b *BackupSpec) MinutelyParams() []string {
	return retentionParams(b.RetentionMinutely, "59")
}
func (b *BackupSpec) HourlyParams() []string {
	return retentionParams(b.RetentionHourly, "59")
}
func (b *BackupSpec) DailyParams() []string {
	return retentionParams(b.RetentionDaily, "23")
}
func (b *BackupSpec) WeeklyParams() []string {
	return retentionParams(b.RetentionWeekly, "7")
}
func (b *BackupSpec) MonthlyParams() []string {
	return retentionParams(b.RetentionMonthly, "L")
}
func (b *BackupSpec) YearlyParams() []string {
	return retentionParams(b.RetentionYearly, "12")
}
