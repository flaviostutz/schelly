package schelly

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

func InitDB() (*sql.DB, error) {
	// prometheus.MustRegister(metricsSQLCounter)

	db0, err := sql.Open("sqlite3", fmt.Sprintf("%s/sqlite.db", opt.DataDir))
	if err != nil {
		return nil, err
	}

	statement, err1 := db0.Prepare("CREATE TABLE IF NOT EXISTS backup_spec (name TEXT, enabled INTEGER NOT NULL, running INTEGER NOT NULL, status TEXT NOT NULL, workflow_name VARCHAR NOT NULL, timeout_seconds INTEGER NOT NULL, workflow_version VARCHAR NOT NULL, parallel_runs INTEGER NOT NULL, checkwarning_seconds INTEGER NOT NULL, start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NOT NULL, last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, minutely VARCHAR NOT NULL DEFAULT '0@L', hourly VARCHAR NOT NULL DEFAULT '0@L', daily VARCHAR NOT NULL DEFAULT '4@L', weekly VARCHAR NOT NULL DEFAULT '4@L', monthly VARCHAR NOT NULL DEFAULT '3@L', yearly VARCHAR NOT NULL DEFAULT '2@L', PRIMARY KEY(`name`))")
	if err1 != nil {
		return nil, err1
	}
	_, err1 = statement.Exec()
	if err1 != nil {
		return nil, err1
	}

	statement, err1 = db0.Prepare("CREATE TABLE IF NOT EXISTS materialized_backup (id TEXT NOT NULL, backup_name TEXT NOT NULL, data_id TEXT, status TEXT NOT NULL, running_delete_workflow TEXT, start_time TIMESTAMP NOT NULL, end_time TIMESTAMP NOT NULL, size REAL, minutely INTEGER NOT NULL DEFAULT 0, hourly INTEGER NOT NULL DEFAULT 0, daily INTEGER NOT NULL DEFAULT 0, weekly INTEGER NOT NULL DEFAULT 0, monthly INTEGER NOT NULL DEFAULT 0, yearly INTEGER NOT NULL DEFAULT 0, reference INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(`id`))")
	if err1 != nil {
		return nil, err1
	}
	_, err1 = statement.Exec()
	if err1 != nil {
		return nil, err1
	}

	os.MkdirAll(opt.DataDir, os.ModePerm)

	logrus.Debug("Database initialized")
	return db0, nil
}
