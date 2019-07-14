package schelly

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func (h *HTTPServer) setupMaterializedHandlers() {
	h.router.GET("/backup/:name/materialized", ListMaterizalized())
	h.router.POST("/backup/:name/materialized", TriggerBackup())
}

//ListMaterizalized get currently tracked backups
func ListMaterizalized() func(*gin.Context) {
	return func(c *gin.Context) {
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

func TriggerBackup() func(*gin.Context) {
	return func(c *gin.Context) {
		logrus.Debugf("TriggerBackup")
		bn := c.Param("name")
		wid, err := triggerNewBackup(bn)
		if err != nil {
			apiInvocationsCounter.WithLabelValues("materialized", "error").Inc()
			c.JSON(http.StatusInternalServerError, gin.H{"message": fmt.Sprintf("Error triggering new backup. err=%s", err)})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"message": fmt.Sprintf("Backup creation scheduled. id=%s", wid)})
		apiInvocationsCounter.WithLabelValues("materialized", "success").Inc()
	}
}
