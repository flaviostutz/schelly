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
	Name              string     `json:"name,omitempty"`
	Enabled           int        `json:"enabled,omitempty"`
	RunningWorkflowID *string    `json:"runningWorkflowId,omitempty"`
	BackupCronString  *string    `json:"backupCronString,omitempty"`
	FromDate          *time.Time `json:"fromDate,omitempty"`
	ToDate            *time.Time `json:"toDate,omitempty"`
	LastUpdate        time.Time  `json:"lastUpdate,omitempty"`
	RetentionMinutely string     `json:"retentionMinutely,omitempty"`
	RetentionHourly   string     `json:"retentionHourly,omitempty"`
	RetentionDaily    string     `json:"retentionDaily,omitempty"`
	RetentionWeekly   string     `json:"retentionWeekly,omitempty"`
	RetentionMonthly  string     `json:"retentionMonthly,omitempty"`
	RetentionYearly   string     `json:"retentionYearly,omitempty"`
}

func createBackupSpec(bs BackupSpec) error {
	stmt, err1 := db.Prepare(`INSERT INTO backup_spec (
								name, enabled, running_workflow_id,
								from_date, to_date, last_update, 
								retention_minutely, retention_hourly, retention_daily, retention_weekly, 
								retention_monthly, retention_yearly, backup_cron_string, retention_cron_string
							) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec(bs.Name, bs.Enabled, bs.RunningWorkflowID,
		bs.FromDate, bs.ToDate, bs.LastUpdate,
		bs.RetentionMinutely, bs.RetentionHourly, bs.RetentionDaily, bs.RetentionWeekly,
		bs.RetentionMonthly, bs.RetentionYearly, bs.BackupCronString)
	if err2 != nil {
		return err2
	}
	return nil
}

func updateBackupSpec(bs BackupSpec) error {
	stmt, err1 := db.Prepare(`UPDATE backup_spec SET
								name=?, enabled=?, running_workflow_id=?,
								from_date=?, to_date=?, last_update=?, 
								retention_minutely=?, retention_hourly=?, retention_daily=?, retention_weekly=?, 
								retention_monthly=?, retention_yearly=?, backup_cron_string=?
							  WHERE name='` + bs.Name + `'`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec(bs.Name, bs.Enabled, bs.RunningWorkflowID,
		bs.FromDate, bs.ToDate, bs.LastUpdate,
		bs.RetentionMinutely, bs.RetentionHourly, bs.RetentionDaily, bs.RetentionWeekly,
		bs.RetentionMonthly, bs.RetentionYearly, bs.BackupCronString)
	if err2 != nil {
		return err2
	}
	return nil
}

func getBackupSpec(backupName string) (BackupSpec, error) {
	rows, err1 := db.Query(`SELECT 
			name, enabled, running_workflow_id,
			from_date, to_date, last_update, 
			retention_minutely, retention_hourly, retention_daily, retention_weekly, 
			retention_monthly, retention_yearly, backup_cron_string
			FROM backup_spec WHERE name='` + backupName + `'`)
	if err1 != nil {
		return BackupSpec{}, err1
	}
	defer rows.Close()

	for rows.Next() {
		b := BackupSpec{}
		err2 := rows.Scan(&b.Name, &b.Enabled, &b.RunningWorkflowID,
			&b.FromDate, &b.ToDate, &b.LastUpdate,
			&b.RetentionMinutely, &b.RetentionHourly, &b.RetentionDaily, &b.RetentionWeekly,
			&b.RetentionMonthly, &b.RetentionYearly, &b.BackupCronString)
		if err2 != nil {
			return BackupSpec{}, err2
		}
		return b, nil
	}
	err := rows.Err()
	if err != nil {
		return BackupSpec{}, err
	}
	return BackupSpec{}, fmt.Errorf("Backup spec name %s not found", backupName)
}

func listBackupSpecs(enabled *int) ([]BackupSpec, error) {
	where := ""
	if enabled != nil {
		where = fmt.Sprintf("WHERE enabled=%d", enabled)
	}
	q := `SELECT 
			name, enabled, running_workflow_id,
			from_date, to_date, last_update, 
			retention_minutely, retention_hourly, retention_daily, retention_weekly, 
			retention_monthly, retention_yearly, backup_cron_string
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
		err2 := rows.Scan(&b.Name, &b.Enabled, &b.RunningWorkflowID,
			&b.FromDate, &b.ToDate, &b.LastUpdate,
			&b.RetentionMinutely, &b.RetentionHourly, &b.RetentionDaily, &b.RetentionWeekly,
			&b.RetentionMonthly, &b.RetentionYearly, &b.BackupCronString)
		if err2 != nil {
			return []BackupSpec{}, err2
		}
		backups = append(backups, b)
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

func updateBackupSpecRunningWorkflowID(backupName string, runningWorkflowID *string) error {
	stmt, err1 := db.Prepare(`UPDATE backup_spec SET
							  running_workflow_id=?
							  WHERE name=?`)
	if err1 != nil {
		return err1
	}
	_, err2 := stmt.Exec(backupName, runningWorkflowID)
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
