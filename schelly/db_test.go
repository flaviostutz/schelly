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

func TestGetAllMaterializedBackups(t *testing.T) {
	initDB()
	bid := strconv.Itoa(rand.Int())
	_, err0 := createMaterializedBackup(bid, "available", time.Now(), time.Now(), "any")
	assert.Nil(t, err0, "err")
	bid = strconv.Itoa(rand.Int())
	_, err0 = createMaterializedBackup(bid, "available", time.Now(), time.Now(), "any")
	assert.Nil(t, err0, "err")
	bid = strconv.Itoa(rand.Int())
	_, err0 = createMaterializedBackup(bid, "available", time.Now(), time.Now(), "any")
	assert.Nil(t, err0, "err")
	backups, err := getAllMaterializedBackups(0)
	assert.Nil(t, err, "err")
	assert.Equal(t, 3, len(backups), "backups")
}
