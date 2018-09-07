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
var getBackupInfoSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "webhook_backup_info_success_total",
	Help: "Total GET /backup/{id} calls success",
})
var getBackupInfoErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "webhook_backup_info_error_total",
	Help: "Total GET /backup/{id} calls error",
})
var getBackupCreateSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "webhook_backup_create_success_total",
	Help: "Total POST /backup calls success",
})
var getBackupCreateErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "webhook_backup_create_error_total",
	Help: "Total POST /backup calls error",
})
var getBackupDeleteSuccessCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "webhook_backup_delete_success_total",
	Help: "Total DELETE /backup/{id} calls success",
})
var getBackupDeleteErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "webhook_backup_delete_error_total",
	Help: "Total DELETE /backup/{id} calls error",
})

//avoid doing webhook operations in parallel
var webhookLock = &sync.Mutex{}

func initWebhook() {
	prometheus.MustRegister(getBackupInfoSuccessCounter)
	prometheus.MustRegister(getBackupInfoErrorCounter)
	prometheus.MustRegister(getBackupCreateSuccessCounter)
	prometheus.MustRegister(getBackupCreateErrorCounter)
	prometheus.MustRegister(getBackupDeleteSuccessCounter)
	prometheus.MustRegister(getBackupDeleteErrorCounter)
}

func getWebhookBackupInfo(backupID string) (ResponseWebhook, error) {
	logrus.Debugf("getWebhookBackupInfo %s - waiting lock", backupID)
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("getWebhookBackupInfo %s - acquired lock", backupID)
	logrus.Debug(fmt.Sprintf("%s/%s", options.webhookURL, backupID))
	resp, data, err := getHTTP(fmt.Sprintf("%s/%s", options.webhookURL, backupID))
	if err != nil {
		logrus.Errorf("Webhook GET backup status invocation failed. err=%s", err)
		getBackupInfoErrorCounter.Inc()
		return ResponseWebhook{}, fmt.Errorf("Webhook GET backup status invocation failed. err=%s", err)
	}
	if resp.StatusCode == 200 {
		var respData ResponseWebhook
		err = json.Unmarshal(data, &respData)
		if err != nil {
			logrus.Errorf("Error parsing json. err=%s", err)
			getBackupInfoErrorCounter.Inc()
			return ResponseWebhook{}, err
		} else {
			getBackupInfoSuccessCounter.Inc()
			return respData, nil
		}
	} else {
		logrus.Warnf("Webhook status != 200 resp=%s", resp)
		getBackupInfoErrorCounter.Inc()
		return ResponseWebhook{}, fmt.Errorf("Couldn't get backup info")
	}
}

func createWebhookBackup() (ResponseWebhook, error) {
	logrus.Debugf("createWebhookBackup - waiting lock")
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("createWebhookBackup - acquired lock")
	resp, data, err := postHTTP(options.webhookURL, options.webhookCreateBody)
	if err != nil {
		logrus.Errorf("Webhook POST new backup invocation failed. err=%s", err)
		getBackupCreateErrorCounter.Inc()
		return ResponseWebhook{}, err
	}
	if resp.StatusCode == 201 {
		var respData ResponseWebhook
		err = json.Unmarshal(data, &respData)
		if err != nil {
			logrus.Errorf("Error parsing json. err=%s", err)
			getBackupCreateErrorCounter.Inc()
			return ResponseWebhook{}, fmt.Errorf("Error parsing json. err=%s", err)
		} else {
			getBackupCreateSuccessCounter.Inc()
			return respData, nil
		}
	} else {
		logrus.Warnf("Webhook status != 201. resp=%s", resp)
		getBackupCreateErrorCounter.Inc()
		return ResponseWebhook{}, fmt.Errorf("Failed to create backup. response")
	}
}

func deleteWebhookBackup(backupID string) error {
	logrus.Debugf("deleteWebhookBackup %s - waiting lock", backupID)
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("deleteWebhookBackup %s - acquired lock", backupID)
	resp, _, err := deleteHTTP(fmt.Sprintf("%s/%s", options.webhookURL, backupID))
	if err != nil {
		logrus.Errorf("Webhook DELETE backup invocation failed. err=%s", err)
		getBackupDeleteErrorCounter.Inc()
		return err
	}
	if resp.StatusCode == 200 {
		logrus.Debugf("Webhook DELETE successful")
		getBackupDeleteSuccessCounter.Inc()
		return nil
	} else if resp.StatusCode == 404 {
		logrus.Warnf("Webhook DELETE appears to be successful. Return was 404 NOT FOUND.")
		getBackupDeleteSuccessCounter.Inc()
		return nil
	} else {
		logrus.Warnf("Webhook status != 200. resp=%s", resp)
		getBackupDeleteErrorCounter.Inc()
		return fmt.Errorf("Webhook status != 200. resp=%v", resp)
	}
}

func postHTTP(url string, data string) (http.Response, []byte, error) {
	req, err := http.NewRequest("POST", options.webhookURL, bytes.NewBuffer([]byte(data)))
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
