package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
)

//METRICS
var backupInfoTimeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "schelly_webhook_backup_info_time_seconds",
	Help: "Time for last POST /backups/{id} call in seconds",
})
var backupInfoSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_webhook_backup_info_success_total",
	Help: "Total GET /backups/{id} calls success",
})
var backupInfoErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_webhook_backup_info_error_total",
	Help: "Total GET /backups/{id} calls error",
})

var backupCreateTimeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "schelly_webhook_backup_create_time_seconds",
	Help: "Time for last POST /backups call in seconds",
})
var backupCreateSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_webhook_backup_create_success_total",
	Help: "Total POST /backups calls success",
})
var backupCreateErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_webhook_backup_create_error_total",
	Help: "Total POST /backups calls error",
})

var backupDeleteTimeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "schelly_webhook_backup_delete_time_seconds",
	Help: "Time for last DELETE /backups/{id} call in seconds",
})
var backupDeleteSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_webhook_backup_delete_success_total",
	Help: "Total DELETE /backups/{id} calls success",
})
var backupDeleteErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "schelly_webhook_backup_delete_error_total",
	Help: "Total DELETE /backups/{id} calls error",
})

//avoid doing webhook operations in parallel
var webhookLock = &sync.Mutex{}

func initWebhook() {
	prometheus.MustRegister(backupInfoTimeGauge)
	prometheus.MustRegister(backupInfoSuccessCounter)
	prometheus.MustRegister(backupInfoErrorCounter)
	prometheus.MustRegister(backupCreateTimeGauge)
	prometheus.MustRegister(backupCreateSuccessCounter)
	prometheus.MustRegister(backupCreateErrorCounter)
	prometheus.MustRegister(backupDeleteTimeGauge)
	prometheus.MustRegister(backupDeleteSuccessCounter)
	prometheus.MustRegister(backupDeleteErrorCounter)
}

func getWebhookBackupInfo(backupID string) (ResponseWebhook, error) {
	logrus.Debugf("getWebhookBackupInfo %s - waiting lock", backupID)
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("getWebhookBackupInfo %s - acquired lock", backupID)
	logrus.Debug(fmt.Sprintf("%s/%s", options.webhookURL, backupID))
	start := time.Now()
	resp, data, err := getHTTP(fmt.Sprintf("%s/%s", options.webhookURL, backupID))
	if err != nil {
		logrus.Errorf("Webhook GET backup status invocation failed. err=%s", err)
		backupInfoErrorCounter.Inc()
		return ResponseWebhook{}, fmt.Errorf("Webhook GET backup status invocation failed. err=%s", err)
	}
	if resp.StatusCode == 200 {
		var respData ResponseWebhook
		err = json.Unmarshal(data, &respData)
		if err != nil {
			logrus.Errorf("Error parsing json. err=%s", err)
			backupInfoErrorCounter.Inc()
			return ResponseWebhook{}, err
		} else {
			backupInfoSuccessCounter.Inc()
			backupInfoTimeGauge.Set(float64(time.Now().Sub(start).Seconds()))
			return respData, nil
		}
	} else {
		logrus.Warnf("Webhook status != 200 resp=%s", resp)
		backupInfoErrorCounter.Inc()
		return ResponseWebhook{}, fmt.Errorf("Couldn't get backup info")
	}
}

func createWebhookBackup() (ResponseWebhook, error) {
	logrus.Debugf("createWebhookBackup - waiting lock")
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("createWebhookBackup - acquired lock")
	start := time.Now()
	resp, data, err := postHTTP(options.webhookURL, options.webhookCreateBody)
	if err != nil {
		logrus.Errorf("Webhook POST new backup invocation failed. err=%s", err)
		backupCreateErrorCounter.Inc()
		return ResponseWebhook{}, err
	}
	if resp.StatusCode == 202 {
		var respData ResponseWebhook
		err = json.Unmarshal(data, &respData)
		if err != nil {
			logrus.Errorf("Error parsing json. err=%s", err)
			backupCreateErrorCounter.Inc()
			return ResponseWebhook{}, fmt.Errorf("Error parsing json. err=%s", err)
		} else {
			backupCreateSuccessCounter.Inc()
			backupCreateTimeGauge.Set(float64(time.Now().Sub(start).Seconds()))
			return respData, nil
		}
	} else {
		logrus.Warnf("Webhook status != 202. resp=%s", resp)
		backupCreateErrorCounter.Inc()
		return ResponseWebhook{}, fmt.Errorf("Failed to create backup. response")
	}
}

func deleteWebhookBackup(backupID string) error {
	logrus.Debugf("deleteWebhookBackup %s - waiting lock", backupID)
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("deleteWebhookBackup %s - acquired lock", backupID)
	start := time.Now()
	resp, _, err := deleteHTTP(fmt.Sprintf("%s/%s", options.webhookURL, backupID))
	if err != nil {
		logrus.Errorf("Webhook DELETE backup invocation failed. err=%s", err)
		backupDeleteErrorCounter.Inc()
		return err
	}
	if resp.StatusCode == 200 {
		logrus.Debugf("Webhook DELETE successful")
		backupDeleteSuccessCounter.Inc()
		backupDeleteTimeGauge.Set(float64(time.Now().Sub(start).Seconds()))
		return nil
	} else if resp.StatusCode == 404 {
		logrus.Warnf("Webhook DELETE appears to be successful. Return was 404 NOT FOUND.")
		backupDeleteSuccessCounter.Inc()
		return nil
	} else {
		logrus.Warnf("Webhook status != 200. resp=%s", resp)
		backupDeleteErrorCounter.Inc()
		return fmt.Errorf("Webhook status != 200. resp=%v", resp)
	}
}

func postHTTP(url string, data string) (http.Response, []byte, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(data)))
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range options.webhookHeaders {
		req.Header.Add(k, v)
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("POST request=%s", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	logrus.Debugf("Response: %s", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}

func getHTTP(url string) (http.Response, []byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}
	for k, v := range options.webhookHeaders {
		req.Header.Add(k, v)
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("GET request=%s", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	logrus.Debugf("Response: %s", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}

func deleteHTTP(url string) (http.Response, []byte, error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}
	for k, v := range options.webhookHeaders {
		req.Header.Add(k, v)
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("DELETE request=%s", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	logrus.Debugf("Response: %s", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}
