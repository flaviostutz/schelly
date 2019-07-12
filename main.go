package main

import (
	"flag"
	"os"
	"strconv"

	"github.com/flaviostutz/schelly/schelly"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"
)

//VERSION schelly version
const VERSION = "1.0.0-beta"

var options schelly.Options

func main() {
	backupName := flag.String("backup-name", "", "Backup name. Required.")
	backupCron := flag.String("backup-cron-string", "", "Cron string used for triggering new backups. If not defined it will be auto generated based on retention configs")
	retentionCron := flag.String("retention-cron-string", "", "Cron string used for triggering retention management tasks. If not defined it will be the same as backup cron string")
	conductorAPIURL := flag.String("conductor-api-url", "", "Base Conductor API URL for calling backup workflows")
	backupTimeout := flag.String("backup-timeout", "", "After this time of a running backup it will be canceled")
	listenPort := flag.Int("listen-port", 8080, "REST API server listen port")
	listenIP := flag.String("listen-ip", "0.0.0.0", "REST API server listen ip address")

	logLevel := flag.String("log-level", "info", "debug, info, warning or error")
	dataDir := flag.String("data-dir", "/var/lib/schelly/data", "debug, info, warning or error")
	versionFlag := flag.String("version", "", "Version info")
	flag.Parse()

	switch *logLevel {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
		break
	case "warning":
		logrus.SetLevel(logrus.WarnLevel)
		break
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
		break
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}

	if *versionFlag != "" {
		logrus.Infof("%s\n", VERSION)
		os.Exit(0)
	}

	logrus.Debug("Preparing options")
	options.BackupName = *backupName
	options.BackupCron = *backupCron
	options.RetentionCron = *retentionCron
	options.ConductorAPIURL = *conductorAPIURL
	options.DataDir = *dataDir
	gts, err2 := strconv.ParseFloat(*backupTimeout, 64)
	options.BackupTimeout = gts
	if err2 != nil {
		logrus.Errorf("backup-timeout has not a valid number. err=%s", err2)
		os.Exit(1)
	}
	options.ListenPort = *listenPort
	options.ListenIP = *listenIP

	if options.BackupName == "" {
		logrus.Error("--backup-name is required")
		os.Exit(1)
	}

	if options.ConductorAPIURL == "" {
		logrus.Error("--conductor-api-url is required")
		os.Exit(1)
	}

	if options.DataDir == "" {
		logrus.Error("--data-dir cannot be empty")
		os.Exit(1)
	}

	logrus.Infof("====Starting Schelly %s====", VERSION)

	schelly.InitAll()
	if err != nil {
		logrus.Errorf("Could not initialized db. err=%s", err)
		os.Exit(1)
	}

	if options.BackupCron == "" {
		logrus.Debug("Generating CRON schedule string")
		options.BackupCron = CalculateCronString(options.MinutelyParams, options.HourlyParams, options.DailyParams, options.WeeklyParams, options.MonthlyParams, options.YearlyParams)
	}

	if options.RetentionCron == "" {
		options.RetentionCron = options.BackupCron
	}

	logrus.Infof("Starting backup cron with schedule '%s'", options.BackupCron)
	logrus.Infof("Starting retention cron with schedule '%s'", options.RetentionCron)

	c := cron.New()
	c.AddFunc(options.BackupCron, func() { schelly.RunBackupTask() })
	c.AddFunc("@every 5s", func() { schelly.CheckBackupTask() })
	c.AddFunc(options.RetentionCron, func() { schelly.RunRetentionTask() })
	c.AddFunc("@every 1d", func() { schelly.RetryDeleteErrors() })
	go c.Start()

	schelly.StartRestAPI()
}

// CalculateCronString calculates a default cron string based on retention time
func CalculateCronString(minutelyParams []string, hourlyParams []string, dailyParams []string, weeklyParams []string, monthlyParams []string, yearlyParams []string) string {
	// Seconds      Minutes      Hours      Day Of Month      Month      Day Of Week      Year
	minutelyRef := minutelyParams[1] + " "
	if minutelyRef == "L " {
		minutelyRef = "59 "
	}

	hourlyRef := hourlyParams[1] + " "
	if hourlyRef == "L " {
		hourlyRef = "59 "
	}

	dailyRef := dailyParams[1] + " "
	if dailyRef == "L " {
		dailyRef = "23 "
	}

	weeklyRef := weeklyParams[1] + " "
	if weeklyRef == "L " {
		weeklyRef = "SAT "
	}

	monthlyRef := monthlyParams[1] + " "

	yearlyRef := yearlyParams[1] + " "
	if yearlyRef == "L " {
		yearlyRef = "12 "
	}

	if minutelyParams[0] != "0" {
		return minutelyRef + "* * * * * *"
	} else if hourlyParams[0] != "0" {
		return minutelyRef + hourlyRef + "* * * * *"
	} else if dailyParams[0] != "0" {
		return minutelyRef + hourlyRef + dailyRef + "* * * *"
	} else if weeklyParams[0] != "0" {
		return minutelyRef + hourlyRef + dailyRef + "* " + "* " + weeklyRef + "*"
	} else if monthlyParams[0] != "0" {
		return minutelyRef + hourlyRef + dailyRef + monthlyRef + "* * *"
		// } else if yearlyParams[0] != "0" {
	} else {
		return minutelyRef + hourlyRef + dailyRef + monthlyRef + yearlyRef + "* *"
	}
}
