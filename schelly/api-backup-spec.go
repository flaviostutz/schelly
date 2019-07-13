package schelly

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func (h *HTTPServer) setupBackupSpecHandlers(opt Options) {
	h.router.GET("/backup/:name", ListBackupSpecs(opt))
	h.router.POST("/backup", CreateBackupSpec(opt))
	h.router.PUT("/backup/:name", UpdatedBackupSpec(opt))
}

//ListMaterizalized get currently tracked backups
func ListBackupSpecs(opt Options) func(*gin.Context) {
	return func(c *gin.Context) {
		parei aqui
		logrus.Debugf("ListMaterizalized")
		tag := c.Query("tag")
		status := c.Query("status")
		name := c.Param("name")

		backups, err := getMaterializedBackups(name, 0, tag, status, false)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error getting materialized. err=%s", err)})
			apiInvocationsCounter.WithLabelValues("materialized", "error").Inc()
			return
		}

		apiInvocationsCounter.WithLabelValues("materialized", "success").Inc()
		c.JSON(http.StatusOK, backups)
	}
}

//TriggerBackup get currently tracked backups
func CreateBackupSpec(opt Options) func(*gin.Context) {
	return func(c *gin.Context) {
		logrus.Debugf("TriggerBackup")
		bn := c.Param("name")
		wid, err := triggerNewBackup(bn)
		if err != nil {
			apiInvocationsCounter.WithLabelValues("error").Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error triggering new backup. err=%s", err)})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"message": fmt.Sprintf("Backup creation scheduled. id=%s", wid)})
		apiInvocationsCounter.WithLabelValues("success").Inc()
	}
}
