package main

import (
	"flag"
	"os"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/robfig/cron"
)

//VERSION schelly version
const VERSION = "1.0.0-beta"

//Options command line options used to run Schelly
type Options struct {
	backupName        string
	backupCron        string
	retentionCron     string
	webhookURL        string
	webhookHeaders    map[string]string
	webhookCreateBody string
	webhookDeleteBody string
	graceTimeSeconds  float64
	dataDir           string
	listenPort        int
	listenIP          string

	minutelyParams []string
	hourlyParams   []string
	dailyParams    []string
	weeklyParams   []string
	monthlyParams  []string
	yearlyParams   []string
}

//ResponseWebhook default response type for webhook invocations
type ResponseWebhook struct {
	ID      string `json:"id",omitempty`
	Status  string `json:"status",omitempty`
	Message string `json:"message",omitempty`
}

var options = new(Options)

func main() {
	backupName := flag.String("backup-name", "", "Backup name. Required.")
	backupCron := flag.String("backup-cron-string", "", "Cron string used for triggering new backups. If not defined it will be auto generated based on retention configs")
	retentionCron := flag.String("retention-cron-string", "", "Cron string used for triggering retention management tasks. If not defined it will be the same as backup cron string")
	webhookURL := flag.String("webhook-url", "", "Base webhook URL for calling backup operations (create/delete backups)")
	webhookHeaders := flag.String("webhook-headers", "", "key=value comma separated list of headers to be sent on backup backend calls")
	webhookCreateBody := flag.String("webhook-create-body", "", "Custom json body to be sent to backup backend webhook when requesting the creation of a new backup")
	webhookDeleteBody := flag.String("webhook-delete-body", "", "Custom json body to be sent to backup backend webhook when requesting the removal of an existing backup")
	graceTimeSeconds := flag.String("webhook-grace-time", "3600", "Minimum time seconds running backup task before trying to cancel it (by calling a /DELETE on the webhook)")
	listenPort := flag.Int("listen-port", 8080, "REST API server listen port")
	listenIP := flag.String("listen-ip", "0.0.0.0", "REST API server listen ip address")

	minutelyRetention := flag.String("retention-minutely", "0", "Minutely retention config")
	hourlyRetention := flag.String("retention-hourly", "1", "Hourly retention config")
	dailyRetention := flag.String("retention-daily", "4@L", "Daily retention config")
	weeklyRetention := flag.String("retention-weekly", "3@L", "Weekly retention config")
	monthlyRetention := flag.String("retention-monthly", "3@L", "Monthly retention config")
	yearlyRetention := flag.String("retention-yearly", "2@L", "Yearly retention config")
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
	options.backupName = *backupName
	options.backupCron = *backupCron
	options.retentionCron = *retentionCron
	options.webhookURL = *webhookURL
	options.webhookCreateBody = *webhookCreateBody
	options.webhookDeleteBody = *webhookDeleteBody
	options.dataDir = *dataDir
	gts, err2 := strconv.ParseFloat(*graceTimeSeconds, 64)
	options.graceTimeSeconds = gts
	if err2 != nil {
		logrus.Errorf("grace-time-seconds has not a valid number. err=%s", err2)
		os.Exit(1)
	}
	options.listenPort = *listenPort
	options.listenIP = *listenIP

	options.minutelyParams = retentionParams(*minutelyRetention, "59")
	options.hourlyParams = retentionParams(*hourlyRetention, "59")
	options.dailyParams = retentionParams(*dailyRetention, "23")
	options.weeklyParams = retentionParams(*weeklyRetention, "7")
	options.monthlyParams = retentionParams(*monthlyRetention, "L")
	options.yearlyParams = retentionParams(*yearlyRetention, "12")

	headers := strings.Split(*webhookHeaders, ",")
	options.webhookHeaders = make(map[string]string)
	if len(headers) > 0 {
		for _, v := range headers {
			headerParts := strings.Split(v, "=")
			if len(headerParts) == 1 {
				logrus.Warnf("Not a complete header k=v tuple %s. Ignoring it.", v)
			} else if len(headerParts) == 2 {
				options.webhookHeaders[strings.Trim(headerParts[0], " ")] = strings.Trim(headerParts[1], " ")
			}
		}
	}

	if options.backupName == "" {
		logrus.Error("--backup-name is required")
		os.Exit(1)
	}

	if options.webhookURL == "" {
		logrus.Error("--webhook-url is required")
		os.Exit(1)
	}

	if options.dataDir == "" {
		logrus.Error("--data-dir cannot be empty")
		os.Exit(1)
	}

	logrus.Infof("====Starting Schelly %s====", VERSION)

	err := initDB()
	if err != nil {
		logrus.Errorf("Could not initialized db. err=%s", err)
		os.Exit(1)
	}

	if options.backupCron == "" {
		logrus.Debug("Generating CRON schedule string")
		options.backupCron = CalculateCronString(options.minutelyParams, options.hourlyParams, options.dailyParams, options.weeklyParams, options.monthlyParams, options.yearlyParams)
	}

	if options.retentionCron == "" {
		options.retentionCron = options.backupCron
	}

	logrus.Infof("Starting backup cron with schedule '%s'", options.backupCron)
	logrus.Infof("Starting retention cron with schedule '%s'", options.retentionCron)

	//for tests
	// runBackupTask()

	c := cron.New()
	c.AddFunc(options.backupCron, func() { runBackupTask() })
	c.AddFunc("@every 1s", func() { checkBackupTask() })
	c.AddFunc(options.retentionCron, func() { runRetentionTask() })
	c.AddFunc("@every 1d", func() { retryDeleteErrors() })
	go c.Start()

	startRestAPI()
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
