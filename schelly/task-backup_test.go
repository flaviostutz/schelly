package schelly

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBackupTagging(t *testing.T) {
	// logrus.SetLevel(logrus.DebugLevel)
	InitDB()

	bid := strconv.Itoa(rand.Int())
	ti, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05Z")
	_, err0 := createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-01T15:04:45Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-01T15:05:01Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-01T16:15:41Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-01T16:45:41Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-01T23:15:31Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-31T10:15:27Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-01-31T20:35:57Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-02-15T13:55:27Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-02-16T17:35:17Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-02-16T18:35:17Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-02-29T08:15:17Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-03-28T09:35:19Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-03-29T04:25:49Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-03-29T19:25:49Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-03-30T21:45:35Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-12-29T11:25:15Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-12-30T16:54:05Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	bid = strconv.Itoa(rand.Int())
	ti, _ = time.Parse(time.RFC3339, "2006-12-31T23:54:05Z")
	_, err0 = createMaterializedBackup(bid, "available", ti, ti, "any", 0)
	assert.Nil(t, err0, "err")

	initMainOptions()
	err0 = tagAllBackups()
	assert.Nil(t, err0, "err")

	//test is performed on samples from 2006
	backups, _ := getMaterializedBackups(0, "", "", false)
	testbackup := keepOnly2006(backups)

	assertTags(t, testbackup[0], true, true, true, true, true, true)
	assertTags(t, testbackup[1], true, true, true, true, false, false)
	assertTags(t, testbackup[2], true, true, true, false, false, false)
	assertTags(t, testbackup[3], true, true, true, true, true, false)
	assertTags(t, testbackup[4], true, true, true, false, false, false)
	assertTags(t, testbackup[5], true, true, false, false, false, false)
	assertTags(t, testbackup[6], true, true, true, false, false, false)
	assertTags(t, testbackup[7], true, true, true, true, true, false)
	assertTags(t, testbackup[8], true, true, false, false, false, false)
	assertTags(t, testbackup[9], true, true, true, false, false, false)
	assertTags(t, testbackup[10], true, true, true, true, true, false)
	assertTags(t, testbackup[11], true, true, false, false, false, false)
	assertTags(t, testbackup[12], true, true, true, true, false, false)
	assertTags(t, testbackup[13], true, true, false, false, false, false)
	assertTags(t, testbackup[14], true, false, false, false, false, false)
	assertTags(t, testbackup[15], true, true, false, false, false, false)
	assertTags(t, testbackup[16], true, false, false, false, false, false)

	backups, err0 = getExclusiveTagAvailableMaterializedBackups("minutely", 0, 999)
	backups = keepOnly2006(backups)
	assert.Nil(t, err0, "err")
	assert.Equal(t, 2, len(backups), "minutely")

	backups, err0 = getExclusiveTagAvailableMaterializedBackups("hourly", 0, 999)
	backups = keepOnly2006(backups)
	assert.Nil(t, err0, "err")
	assert.Equal(t, 5, len(backups), "hourly")

	backups, err0 = getExclusiveTagAvailableMaterializedBackups("daily", 0, 999)
	backups = keepOnly2006(backups)
	assert.Nil(t, err0, "err")
	assert.Equal(t, 4, len(backups), "daily")

	backups, err0 = getExclusiveTagAvailableMaterializedBackups("weekly", 0, 999)
	backups = keepOnly2006(backups)
	assert.Nil(t, err0, "err")
	assert.Equal(t, 2, len(backups), "weekly")

	backups, err0 = getExclusiveTagAvailableMaterializedBackups("monthly", 0, 999)
	backups = keepOnly2006(backups)
	assert.Nil(t, err0, "err")
	assert.Equal(t, 3, len(backups), "monthly")

	backups, err0 = getExclusiveTagAvailableMaterializedBackups("yearly", 0, 999)
	backups = keepOnly2006(backups)
	assert.Nil(t, err0, "err")
	assert.Equal(t, 1, len(backups), "yearly")

	showAllBackups()

}

func assertTags(t *testing.T, backup MaterializedBackup, minutely bool, hourly bool, daily bool, weekly bool, monthly bool, yearly bool) {
	assert.Equal(t, minutely, backup.Minutely == 1, "minutely")
	assert.Equal(t, hourly, backup.Hourly == 1, "hourly")
	assert.Equal(t, daily, backup.Daily == 1, "daily")
	assert.Equal(t, weekly, backup.Weekly == 1, "weekly")
	assert.Equal(t, monthly, backup.Monthly == 1, "monthly")
	assert.Equal(t, yearly, backup.Yearly == 1, "yearly")
}

func initMainOptions() {
	options = &Options{}

	// backupName 		= "",
	// backupCron 		= "",
	// retentionCron 	= "",
	// webhookURL 		= "",
	// webhookHeaders 	= "",
	// webhookCreateBody = "",
	// webhookDeleteBody = "",
	// graceTimeSeconds = "",
	// dataDir 		= "",
	options.MinutelyParams = []string{"2", "59"}
	options.HourlyParams = []string{"3", "59"}
	options.DailyParams = []string{"3", "23"}
	options.WeeklyParams = []string{"4", "7"}
	options.MonthlyParams = []string{"5", "L"}
	options.YearlyParams = []string{"2", "12"}
}

func showAllBackups() {
	backups, _ := getMaterializedBackups(0, "", "", false)
	for _, b := range backups {
		info := fmt.Sprintf("%s ", b.StartTime)
		if b.Reference == 1 {
			info += "Reference "
		}
		if b.Minutely == 1 {
			info += "Minutely "
		}
		if b.Hourly == 1 {
			info += "Hourly "
		}
		if b.Daily == 1 {
			info += "Daily "
		}
		if b.Weekly == 1 {
			info += "Weekly "
		}
		if b.Monthly == 1 {
			info += "Monthly "
		}
		if b.Yearly == 1 {
			info += "Yearly "
		}
		logrus.Debugf("%s", info)
	}
}

func keepOnly2006(backups []MaterializedBackup) []MaterializedBackup {
	testbackup := make([]MaterializedBackup, 0)
	for _, b := range backups {
		if b.StartTime.Year() == 2006 {
			testbackup = append(testbackup, b)
		}
	}
	return testbackup
}
