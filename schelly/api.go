package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var apiInvocationsSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_api_invocations_success_total",
	Help: "Total api requests served with success",
})

var apiInvocationsErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_api_invocations_error_total",
	Help: "Total api requests served with error",
})

func startRestAPI() {
	prometheus.MustRegister(apiInvocationsSuccessCounter)
	prometheus.MustRegister(apiInvocationsErrorCounter)

	router := mux.NewRouter()
	router.HandleFunc("/backups", GetBackups).Methods("GET")
	router.HandleFunc("/backups", TriggerBackup).Methods("POST")
	router.Handle("/metrics", promhttp.Handler())
	listen := fmt.Sprintf("%s:%d", options.listenIP, options.listenPort)
	logrus.Infof("Listening at %s", listen)
	err := http.ListenAndServe(listen, router)
	if err != nil {
		logrus.Errorf("Error while listening requests: %s", err)
		os.Exit(1)
	}
}

//GetBackups get currently tracked backups
func GetBackups(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("GetBackups r=%s", r)
	tag := r.URL.Query().Get("tag")
	status := r.URL.Query().Get("status")
	backups, err := getMaterializedBackups(0, tag, status, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		apiInvocationsErrorCounter.Inc()
		return
	}

	rjson := ""
	for _, b := range backups {
		tags := ""

		bt := getTags(b)
		for _, tag := range bt {
			if tags != "" {
				tags = tags + ","
			}
			tags = tags + "\"" + tag + "\""
		}

		if rjson != "" {
			rjson = rjson + ","
		}
		rjson = rjson + "{\"id\":\"" + b.ID + "\", \"data_id\":\"" + b.DataID + "\", \"status\":\"" + b.Status + "\", \"start_time\":\"" + fmt.Sprintf("%s", b.StartTime) + "\", \"end_time\":\"" + fmt.Sprintf("%s", b.EndTime) + "\", \"size\":\"" + fmt.Sprintf("%f", b.SizeMB) + "\", \"custom_data\":\"" + b.CustomData + "\", \"tags\":[" + tags + "]}"
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("[" + rjson + "]"))
	logrus.Debugf("result: %s", "["+rjson+"]")
	apiInvocationsSuccessCounter.Inc()
}

//TriggerBackup get currently tracked backups
func TriggerBackup(w http.ResponseWriter, r *http.Request) {
	logrus.Debugf("TriggerBackup r=%s", r)
	result, err := triggerNewBackup()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		apiInvocationsErrorCounter.Inc()
		return
	}
	w.Header().Set("Content-Type", "application/json")
	rs := "{}"
	if result.ID != "" {
		rs = "{id:'" + result.ID + "',status:'" + result.Status + "',message:'" + result.Message + "'}"
	}
	w.Write([]byte(rs))
	logrus.Debugf("result: %s", rs)
	apiInvocationsSuccessCounter.Inc()
}
