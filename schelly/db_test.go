package main

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStoreTask1(t *testing.T) {
	initDB()
	err := setCurrentTaskStatus("abc", "pending", time.Now())
	assert.Nil(t, err, "err")
	backupID, backupStatus, backupTime, err1 := getCurrentTaskStatus()
	assert.Nil(t, err1, "err1")
	assert.Equal(t, backupID, "abc", "backupID")
	assert.Equal(t, backupStatus, "pending", "backupStatus")
	assert.Truef(t, backupTime.Sub(time.Now()) < 10000, "backupTime %s", backupTime)
}

func TestStoreTask2(t *testing.T) {
	initDB()
	err := setCurrentTaskStatus("xyz", "success", time.Now())
	assert.Nil(t, err, "err")
	backupID, backupStatus, backupTime, err1 := getCurrentTaskStatus()
	assert.Nil(t, err1, "err1")
	assert.Equal(t, backupID, "xyz", "backupID")
	assert.Equal(t, backupStatus, "success", "backupStatus")
	assert.Truef(t, backupTime.Sub(time.Now()) < 10000, "backupTime %s", backupTime)
}

func TestGetMaterializedBackups(t *testing.T) {
	initDB()
	bid := strconv.Itoa(rand.Int())
	_, err0 := createMaterializedBackup(bid, bid, "abc", time.Now(), time.Now(), "any", 0)
	assert.Nil(t, err0, "err")
	bid = strconv.Itoa(rand.Int())
	_, err0 = createMaterializedBackup(bid, bid, "def", time.Now(), time.Now(), "any", 0)
	assert.Nil(t, err0, "err")
	bid = strconv.Itoa(rand.Int())
	_, err0 = createMaterializedBackup(bid, bid, "ghi", time.Now(), time.Now(), "any", 0)
	assert.Nil(t, err0, "err")
	backups, err := getMaterializedBackups(0, "", "", false)
	assert.Nil(t, err, "err")
	assert.Equal(t, 3, len(backups), "backups")
}

func TestGetFilteredMaterializedBackups(t *testing.T) {
	initDB()
	bid := strconv.Itoa(rand.Int())
	_, err0 := createMaterializedBackup(bid, bid+"1", "123", time.Now(), time.Now(), "any", 0)
	assert.Nil(t, err0, "err")
	bid = strconv.Itoa(rand.Int())
	_, err0 = createMaterializedBackup(bid, bid+"1", "456", time.Now(), time.Now(), "any", 0)
	assert.Nil(t, err0, "err")
	bid = strconv.Itoa(rand.Int())
	_, err0 = createMaterializedBackup(bid, bid+"1", "456", time.Now(), time.Now(), "any", 0)
	assert.Nil(t, err0, "err")
	backups, err := getMaterializedBackups(0, "", "123", false)
	assert.Nil(t, err, "err")
	assert.Equal(t, 1, len(backups), "backups")
	backups, err = getMaterializedBackups(0, "", "456", true)
	assert.Nil(t, err, "err")
	assert.Equal(t, 2, len(backups), "backups")
	assert.Equal(t, backups[0].ID+"1", backups[0].DataID, "backups")
	assert.Equal(t, backups[1].ID+"1", backups[1].DataID, "backups")
	assert.Equal(t, backups[2].ID+"1", backups[2].DataID, "backups")
}
