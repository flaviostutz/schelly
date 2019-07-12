package schelly

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

var apiInvocationsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "schelly_api_invocations_total",
	Help: "Total api requests served",
}, []string{
	"entity",
	"status",
})

func (h *HTTPServer) setupMaterializedHandlers(opt Options) {
	prometheus.MustRegister(apiInvocationsCounter)
	h.router.GET("/backup/:name/materialized", ListMaterizalized(opt))
	h.router.POST("/backup/:name/materialized", TriggerBackup(opt))
}

//ListMaterizalized get currently tracked backups
func ListMaterizalized(opt Options) func(*gin.Context) {
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

//TriggerBackup get currently tracked backups
func TriggerBackup(opt Options) func(*gin.Context) {
	return func(c *gin.Context) {
		logrus.Debugf("TriggerBackup")
		bn := c.Param("name")
		result, err := triggerNewBackup(bn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			apiInvocationsCounter.WithLabelValues("error").Inc()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		rs := "{}"
		if result.ID != "" {
			rs = "{id:'" + result.ID + "',status:'" + result.Status + "',message:'" + result.Message + "'}"
		}
		w.Write([]byte(rs))
		logrus.Debugf("result: %s", rs)
		apiInvocationsCounter.WithLabelValues("success").Inc()
	}
}
