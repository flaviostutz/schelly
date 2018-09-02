package main

import (
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
	_, err0 := createMaterializedBackup("abc", "available", time.Now(), time.Now(), "any")
	assert.Nil(t, err0, "err")
	_, err0 = createMaterializedBackup("123", "available", time.Now(), time.Now(), "any")
	assert.Nil(t, err0, "err")
	_, err0 = createMaterializedBackup("xyz", "available", time.Now(), time.Now(), "any")
	assert.Nil(t, err0, "err")
	backups, err := getAllMaterializedBackups(0)
	assert.Nil(t, err, "err")
	assert.Equal(t, len(backups), 3, "backups")
}

// func TestMarkAsReferenceMaterializedBackup1(t *testing.T) {
// 	initDB()
// 	bid := strconv.Itoa(rand.Int())
// 	_, err0 := createMaterializedBackup(bid, "available", time.Now(), time.Now(), "any")
// 	assert.Nil(t, err0, "err")
// 	err0 = markAsReferenceMaterializedBackup(bid, true)
// 	assert.Nil(t, err0, "err")
// 	backup, err1 := getMaterializedBackup(bid)
// 	assert.Nil(t, err1, "err")
// 	assert.Equal(t, backup.ID, bid)
// 	assert.Equal(t, backup.IsReference, 1)
// }

// func TestMarkAsReferenceMaterializedBackup2(t *testing.T) {
// 	initDB()
// 	bid := strconv.Itoa(rand.Int())
// 	_, err0 := createMaterializedBackup(bid, "available", time.Now(), time.Now(), "any")
// 	assert.Nil(t, err0, "err")
// 	err0 = markAsReferenceMaterializedBackup(bid, false)
// 	assert.Nil(t, err0, "err")
// 	backup, err1 := getMaterializedBackup(bid)
// 	assert.Nil(t, err1, "err")
// 	assert.Equal(t, backup.ID, bid)
// 	assert.Equal(t, backup.IsReference, 0)
// 	is, err2 := isReferenceMaterializedBackup(bid)
// 	assert.Nil(t, err2, "err")
// 	assert.False(t, is, "false")
// 	err0 = markAsReferenceMaterializedBackup(bid, true)
// 	assert.Nil(t, err0, "err")
// 	is, err2 = isReferenceMaterializedBackup(bid)
// 	assert.Nil(t, err2, "true")
// 	assert.True(t, is, "false")
// }

// func TestAddRemoveTagMaterializedBackup1(t *testing.T) {
// 	initDB()
// 	bid := strconv.Itoa(rand.Int())
// 	_, err0 := createMaterializedBackup(bid, "available", time.Now(), time.Now(), "any")

// 	assert.Nil(t, err0, "err")
// 	err0 = addTagMaterializedBackup(bid, "monthly")
// 	assert.Nil(t, err0, "err")

// 	err0 = removeTagMaterializedBackup(bid, "monthly")
// 	assert.Nil(t, err0, "err")

// 	err0 = addTagMaterializedBackup(bid, "minutely")
// 	assert.Nil(t, err0, "err")
// 	err0 = addTagMaterializedBackup(bid, "monthly")
// 	assert.Nil(t, err0, "err")
// 	backup, err1 := getMaterializedBackup(bid)
// 	assert.Nil(t, err1, "err")
// 	assert.True(t, strings.Contains(backup.Tags, "minutely"), "contains tags")
// 	assert.True(t, strings.Contains(backup.Tags, "monthly"), "contains tags")
// 	assert.False(t, strings.Contains(backup.Tags, "yearly"), "contains tags")

// 	err0 = addTagMaterializedBackup(bid, "yearly")
// 	assert.Nil(t, err0, "err")
// 	backup, err1 = getMaterializedBackup(bid)
// 	assert.Nil(t, err1, "err")
// 	assert.True(t, strings.Contains(backup.Tags, "yearly"), "contains tags")

// 	err0 = removeTagMaterializedBackup(bid, "minutely")
// 	assert.Nil(t, err0, "err")
// 	err0 = removeTagMaterializedBackup(bid, "monthly")
// 	assert.Nil(t, err0, "err")
// 	err0 = removeTagMaterializedBackup(bid, "monthly")
// 	assert.NotNil(t, err0, "err")
// 	err0 = removeTagMaterializedBackup(bid, "yearly")
// 	assert.Nil(t, err0, "err")
// 	backup, err1 = getMaterializedBackup(bid)
// 	assert.Nil(t, err1, "err")
// 	assert.False(t, strings.Contains(backup.Tags, "yearly"), "contains tags")
// 	assert.Equal(t, backup.Tags, "", "empty tags")

// 	err1 = removeTagMaterializedBackup(bid, "monthly")
// 	assert.NotNil(t, err1, "err")
// 	backup, err1 = getMaterializedBackup(bid)
// 	assert.Nil(t, err1, "err")
// 	assert.Equal(t, backup.ID, bid)
// 	assert.Equal(t, backup.IsReference, 0)
// }
