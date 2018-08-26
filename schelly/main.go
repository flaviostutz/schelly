//This is a hard fork from the great job done by 
//http://github.com/yp-engineering/rbd-docker-plugin
package main

import (
	"flag"
	"strings"
	"os"
	"github.com/Sirupsen/logrus"
	"github.com/robfig/cron"
)

const VERSION = "1.0.0-beta"

type Options struct {
	backupName string
	backupCron string
	webhookUrl string
	webhookHeaders string
	webhookCreateBody string
	webhookDeleteBody string
	graceTimeSeconds string

	secondlyParams []string
	minutelyParams []string
	hourlyParams []string
	dailyParams []string
	weeklyParams []string
	monthlyParams []string
	yearlyParams []string
}

var options = new(Options)

func main() {
	backupName            := flag.String("backup-name", "", "Backup name. Required.")
	backupCron            := flag.String("backup-cron-string", "", "Cron string used for triggering new backups. If not defined it will be auto generated based on retention configs")
	webhookUrl            := flag.String("webhook-url", "", "Base webhook URL for calling backup operations (create/delete backups)")
	webhookHeaders        := flag.String("webhook-headers", "", "key=value comma separated list of headers to be sent on backup backend calls")
	webhookCreateBody     := flag.String("webhook-create-body", "", "Custom json body to be sent to backup backend webhook when requesting the creation of a new backup")
	webhookDeleteBody     := flag.String("webhook-delete-body", "", "Custom json body to be sent to backup backend webhook when requesting the removal of an existing backup")
	graceTimeSeconds      := flag.String("webhook-grace-time", "", "Minimum time running backup task before trying to cancel it (by calling a /DELETE on the webhook)")

	secondlyRetention     := flag.String("retention-secondly", "0", "Secondly retention config")
	minutelyRetention     := flag.String("retention-minutely", "0", "Minutely retention config")
	hourlyRetention       := flag.String("retention-hourly", "0", "Hourly retention config")
	dailyRetention        := flag.String("retention-daily", "4@L", "Daily retention config")
	weeklyRetention       := flag.String("retention-weekly", "3@L", "Weekly retention config")
	monthlyRetention      := flag.String("retention-monthly", "3@L", "Monthly retention config")
	yearlyRetention       := flag.String("retention-yearly", "2@L", "Yearly retention config")
	logLevel              := flag.String("log-level", "info", "debug, info, warning or error")
	versionFlag           := flag.String("version", "", "Version info")
	flag.Parse()

	switch *logLevel {
		case "debug":
			logrus.SetLevel(logrus.DebugLevel)
			break;
		case "warning":
			logrus.SetLevel(logrus.WarnLevel)
			break;
		case "error":
			logrus.SetLevel(logrus.ErrorLevel)
			break;
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
	options.webhookUrl = *webhookUrl
	options.webhookHeaders = *webhookHeaders
	options.webhookCreateBody = *webhookCreateBody
	options.webhookDeleteBody = *webhookDeleteBody
	options.graceTimeSeconds = *graceTimeSeconds

	options.secondlyParams = retentionParams(*secondlyRetention)
	options.minutelyParams = retentionParams(*minutelyRetention)
	options.hourlyParams = retentionParams(*hourlyRetention)
	options.dailyParams = retentionParams(*dailyRetention)
	options.weeklyParams = retentionParams(*weeklyRetention)
	options.monthlyParams = retentionParams(*monthlyRetention)
	options.yearlyParams = retentionParams(*yearlyRetention)

	if options.backupName == "" {
		logrus.Error("--backup-name is required")
		os.Exit(1)
	}

	if options.webhookUrl == "" {
		logrus.Error("--webhook-url is required")
		os.Exit(1)
	}

	logrus.Infof("====Starting Schelly %s====", VERSION)

	scheduleCronString := *backupCron
	if scheduleCronString == "" {
		logrus.Debug("Generating CRON schedule string")
		scheduleCronString = calculateCronString(options.secondlyParams, options.minutelyParams, options.hourlyParams, options.dailyParams, options.weeklyParams, options.monthlyParams, options.yearlyParams)
	}

	logrus.Infof("Cron schedule string is '%s'. Starting...", scheduleCronString)
	c := cron.New()
	c.AddFunc(scheduleCronString, func() {triggerTasks()})
	c.Start()
}

func triggerTasks() {
	logrus.Info("Starting backup tasks...")
	runBackupTask()
	runRetentionTask()
}

func calculateCronString(secondlyParams []string, minutelyParams []string, hourlyParams []string, dailyParams []string, weeklyParams[]string, monthlyParams []string, yearlyParams []string) string {
	// Seconds      Minutes      Hours      Day Of Month      Month      Day Of Week      Year
	minutelyRef := minutelyParams[1] + " "
	if minutelyRef=="L" { minutelyRef = "59 " }

	hourlyRef := hourlyParams[1] + " "
	if hourlyRef=="L" { hourlyRef = "59 " }

	dailyRef := dailyParams[1] + " "
	if dailyRef=="L" { dailyRef = "23 " }

	weeklyRef := weeklyParams[1] + " "
	if weeklyRef=="L" { weeklyRef = "7 " }

	monthlyRef := monthlyParams[1] + " "

	yearlyRef := yearlyParams[1] + " "
	if yearlyRef=="L" { yearlyRef = "12 " }
	
	if secondlyParams[0] != "0" {
		return "* * * * * * *"
	} else if minutelyParams[0] != "0" {
		return minutelyRef + "* * * * * *"
	} else if hourlyParams[0] != "0" {
		return minutelyRef + hourlyRef + "* * * * *"
	} else if dailyParams[0] != "0" {
		return minutelyRef + hourlyRef + dailyRef + "* * * *"
	} else if monthlyParams[0] != "0" {
		return minutelyRef + hourlyRef + dailyRef + monthlyRef + "* * *"
	} else if weeklyParams[0] != "0" {
		return minutelyRef + hourlyRef + dailyRef + "* " + "* " +  weeklyRef + "*"
	// } else if yearlyParams[0] != "0" {
	} else {
		return minutelyRef + hourlyRef + dailyRef + monthlyRef + yearlyRef + "* *"
	}
}

func retentionParams(config string) ([]string) {
	params := strings.Split(config, "@")
	if len(params)==0 {
		params = append(params, "0")
	}
	if len(params)==1 {
		params = append(params, "L")
	}
	return params
}
