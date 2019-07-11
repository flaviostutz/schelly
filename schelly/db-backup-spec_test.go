package schelly

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStoreBackupSpec(t *testing.T) {
	InitDB()
	bid := "uid1"
	bs := BackupSpec{
		ID:                bid,
		Name:              "test1",
		Enabled:           true,
		FromDate:          nil,
		ToDate:            nil,
		WorkflowName:      "wf1",
		WorkflowVersion:   "1",
		RetentionMinutely: "2@L",
		RetentionHourly:   "2@L",
		LastUpdate:        time.Now(),
	}
	err := createBackupSpec(bs)
	assert.Nil(t, err, "err")
	bs1, err1 := getBackupSpec(bs.ID)
	assert.Nil(t, err1, "err1")
	assert.Equal(t, bs1.ID, bid, "ID")
	assert.Equal(t, bs1.Name, bs.Name, "Name")
	assert.Equal(t, bs1.RetentionMinutely, "2@L")
	assert.True(t, bs1.Enabled, "Enabled")
	assert.Nil(t, bs1.RetentionMonthly, "monthly")
}
