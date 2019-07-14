package schelly

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func (h *HTTPServer) setupBackupSpecHandlers() {
	h.router.GET("/backup/:name", ListBackupSpecs())
	h.router.POST("/backup", CreateBackupSpec())
	h.router.PUT("/backup/:name", UpdateBackupSpec())
	// h.router.DELETE("/backup/:name", DeleteBackupSpec())
}

//ListBackupSpecs list
func ListBackupSpecs() func(*gin.Context) {
	return func(c *gin.Context) {
		logrus.Debugf("ListBackupSpecs")

		var enabled *int
		e := c.Query("enabled")
		if e != "" {
			en, err := strconv.Atoi(e)
			if err != nil {
				c.JSON(http.StatusBadRequest, "Query param 'enabled' must be 0 or 1")
				return
			}
			enabled = &en
		}
		backups, err := listBackupSpecs(enabled)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error getting backup specs. err=%s", err)})
			apiInvocationsCounter.WithLabelValues("backup-spec", "error").Inc()
			return
		}

		apiInvocationsCounter.WithLabelValues("backup-spec", "success").Inc()
		c.JSON(http.StatusOK, backups)
	}
}

//CreateBackupSpec create
func CreateBackupSpec() func(*gin.Context) {
	return func(c *gin.Context) {
		logrus.Debugf("CreateBackupSpec")

		bs := BackupSpec{}
		data, _ := ioutil.ReadAll(c.Request.Body)
		err := json.Unmarshal(data, &bs)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("Invalid backup spec. err=%s", err)})
			return
		}

		if bs.Name == "" {
			c.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("'name' is required")})
			return
		}

		setBackupSpecDefaultValues(&bs)
		bs.LastUpdate = time.Now()

		err = createBackupSpec(bs)
		if err != nil {
			apiInvocationsCounter.WithLabelValues("backup-spec", "error").Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error creating backup spec. err=%s", err)})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"message": fmt.Sprintf("Backup spec created. name=%s", bs.Name)})
		apiInvocationsCounter.WithLabelValues("backup-spec", "success").Inc()
	}
}

//UpdateBackupSpec get currently tracked backups
func UpdateBackupSpec() func(*gin.Context) {
	return func(c *gin.Context) {
		logrus.Debugf("UpdateBackupSpec")
		name := c.Param("name")

		bs := BackupSpec{}
		data, _ := ioutil.ReadAll(c.Request.Body)
		err := json.Unmarshal(data, &bs)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"message": fmt.Sprintf("Invalid backup spec. err=%s", err)})
			return
		}
		bs.Name = name

		setBackupSpecDefaultValues(&bs)
		bs.LastUpdate = time.Now()

		err = updateBackupSpec(bs)
		if err != nil {
			apiInvocationsCounter.WithLabelValues("backup-spec", "error").Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error updating backup spec. err=%s", err)})
			return
		}
		c.JSON(http.StatusCreated, gin.H{"message": fmt.Sprintf("Backup spec updated. name=%s", bs.Name)})
		apiInvocationsCounter.WithLabelValues("backup-spec", "success").Inc()
	}
}

//DeleteBackupSpec get currently tracked backups
// func DeleteBackupSpec() func(*gin.Context) {
// 	return func(c *gin.Context) {
// 		logrus.Debugf("DeleteBackupSpec")
// 		name := c.Param("name")

// 		err := deleteBackupSpec(name)
// 		if err != nil {
// 			apiInvocationsCounter.WithLabelValues("backup-spec", "error").Inc()
// 			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error deleting backup spec. err=%s", err)})
// 			return
// 		}
// 		c.JSON(http.StatusCreated, gin.H{"message": fmt.Sprintf("Backup spec deleted. name=%s", name)})
// 		apiInvocationsCounter.WithLabelValues("backup-spec", "success").Inc()
// 	}
// }

func setBackupSpecDefaultValues(bs *BackupSpec) {
	if bs.RetentionMinutely == "" {
		bs.RetentionMinutely = "0@L"
	}
	if bs.RetentionHourly == "" {
		bs.RetentionHourly = "0@L"
	}
	if bs.RetentionDaily == "" {
		bs.RetentionDaily = "4@L"
	}
	if bs.RetentionWeekly == "" {
		bs.RetentionWeekly = "4@L"
	}
	if bs.RetentionMonthly == "" {
		bs.RetentionMonthly = "3@L"
	}
	if bs.RetentionYearly == "" {
		bs.RetentionYearly = "2@L"
	}

	if bs.BackupCronString == nil {
		cp := calculateCronString(bs.MinutelyParams(), bs.HourlyParams(), bs.DailyParams(), bs.WeeklyParams(), bs.MonthlyParams(), bs.YearlyParams())
		bs.BackupCronString = &cp
	}
}

// CalculateCronString calculates a default cron string based on retention time
func calculateCronString(minutelyParams []string, hourlyParams []string, dailyParams []string, weeklyParams []string, monthlyParams []string, yearlyParams []string) string {
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
