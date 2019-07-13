package schelly

import "database/sql"

var (
	opt         Options
	db          *sql.DB
	backupTasks map[string]BackupTask
)

//Options command line options used to run Schelly
type Options struct {
	BackupName      string
	BackupCron      string
	RetentionCron   string
	ConductorAPIURL string
	DataDir         string
	ListenPort      int
	ListenIP        string
}

func InitAll(opt0 Options) error {
	opt = opt0

	InitConductor()
	db0, err := InitDB()
	if err != nil {
		return err
	}

	db = db0

	refreshTasks()
	// InitBackup()
	// InitRetention()
}

func refreshTasks() {

}
