package schelly

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

//METRICS
var invocationHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "schelly_conductor_invocation",
	Help:    "Total duration of Conductor calls",
	Buckets: []float64{0.1, 1, 10},
}, []string{
	// which webhook operation?
	"operation",
	// webhook operation result
	"status",
})

//avoid doing conductor operations in parallel
var conductorLock = &sync.Mutex{}

func InitConductor() {
	prometheus.MustRegister(invocationHist)
}

func launchWorkflow(backupName string) error {
	logrus.Debugf("startWorkflow backupName=%s", backupName)

	logrus.Debugf("Loading backup definition from DB")
	bs := getBackupSpec(backupName)

	wf := make(map[string]interface{})
	wf["name"] = schedule.WorkflowName
	wf["version"] = schedule.WorkflowVersion
	wf["input"] = schedule.WorkflowContext
	wf["input"].(map[string]interface{})["backupName"] = schedule.Name
	wfb, _ := json.Marshal(wf)

	logrus.Debugf("Launching Workflow %s", wf)
	url := fmt.Sprintf("%s/workflow", conductorURL)
	resp, data, err := postHTTP(url, wfb)
	if err != nil {
		logrus.Errorf("Call to Conductor POST /workflow failed. err=%s", err)
		return err
	}
	if resp.StatusCode != 200 {
		logrus.Warnf("POST /workflow call status!=200. resp=%v", resp)
		return fmt.Errorf("Failed to create new workflow instance. status=%d", resp.StatusCode)
	}
	logrus.Infof("Schedule %s: Workflow %s launched. workflowId=%s", schedule.Name, schedule.WorkflowName, string(data))
	return nil
}

func getWorkflow(name string, version string) (map[string]interface{}, error) {
	logrus.Debugf("getWorkflow %s", name)
	resp, data, err := getHTTP(fmt.Sprintf("%s/metadata/workflow/%s?version=%s", conductorURL, name, version))
	if err != nil {
		return nil, fmt.Errorf("GET /metadata/workflow/name failed. err=%s", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Couldn't get workflow info. name=%s", name)
	}
	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return nil, err
	}
	return wfdata, nil
}

func getWorkflowInstance(workflowID string) (map[string]interface{}, error) {
	logrus.Debugf("getWorkflowInstance %s", workflowID)
	resp, data, err := getHTTP(fmt.Sprintf("%s/workflow/%s?includeTasks=false", conductorURL, workflowID))
	if err != nil {
		return nil, fmt.Errorf("GET /workflow/%s?includeTasks=false failed. err=%s", err, workflowID)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Couldn't get workflow info. workflowId=%s. status=%d", workflowID, resp.StatusCode)
	}
	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return nil, err
	}
	return wfdata, nil
}

func findWorkflows(backupName string, running bool) (map[string]interface{}, error) {
	logrus.Debugf("findWorkflows %s", backupName)
	runstr := ""
	if running {
		runstr = " AND status=RUNNING"
	} else {
		runstr = " AND NOT status=RUNNING"
	}
	freeText := fmt.Sprintf("backupName=%s%s", backupName, runstr)
	sr := fmt.Sprintf("%s/workflow/search?freeText=%s&sort=endTime:DESC&size=5", conductorURL, url.QueryEscape(freeText))
	// logrus.Debugf("WORKFLOW SEARCH URL=%s", sr)
	resp, data, err := getHTTP(sr)
	if err != nil {
		return nil, fmt.Errorf("GET /workflow/search failed. err=%s", err)
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET /workflow/search failed. status=%d. err=%s", resp.StatusCode, err)
	}
	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return nil, err
	}
	return wfdata, nil
}

func getWebhookBackupInfo(backupID string) (ResponseWebhook, error) {
	logrus.Debugf("getWebhookBackupInfo %s - waiting lock", backupID)
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("getWebhookBackupInfo %s - acquired lock", backupID)
	logrus.Debug(fmt.Sprintf("%s/%s", options.WebhookURL, backupID))
	start := time.Now()
	resp, data, err := getHTTP(fmt.Sprintf("%s/%s", options.WebhookURL, backupID))
	if err != nil {
		logrus.Errorf("Webhook GET backup status invocation failed. err=%s", err)
		invocationHist.WithLabelValues("info", "error").Observe(float64(time.Since(start).Seconds()))
		return ResponseWebhook{}, fmt.Errorf("Webhook GET backup status invocation failed. err=%s", err)
	}
	if resp.StatusCode == 200 {
		var respData ResponseWebhook
		err = json.Unmarshal(data, &respData)
		if err != nil {
			logrus.Errorf("Error parsing json. err=%s", err)
			invocationHist.WithLabelValues("info", "error").Observe(float64(time.Since(start).Seconds()))
			return ResponseWebhook{}, err
		} else {
			invocationHist.WithLabelValues("info", "success").Observe(float64(time.Since(start).Seconds()))
			return respData, nil
		}
	} else {
		logrus.Warnf("Webhook status != 200 resp=%s", resp)
		invocationHist.WithLabelValues("info", "error").Observe(float64(time.Since(start).Seconds()))
		return ResponseWebhook{}, fmt.Errorf("Couldn't get backup info")
	}
}

func createWebhookBackup() (ResponseWebhook, error) {
	logrus.Debugf("createWebhookBackup - waiting lock")
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("createWebhookBackup - acquired lock")
	start := time.Now()
	resp, data, err := postHTTP(options.WebhookURL, options.WebhookCreateBody)
	if err != nil {
		logrus.Errorf("Webhook POST new backup invocation failed. err=%s", err)
		invocationHist.WithLabelValues("create", "error").Observe(float64(time.Since(start).Seconds()))
		return ResponseWebhook{}, err
	}
	if resp.StatusCode == 202 {
		var respData ResponseWebhook
		err = json.Unmarshal(data, &respData)
		if err != nil {
			logrus.Errorf("Error parsing json. err=%s", err)
			invocationHist.WithLabelValues("create", "error").Observe(float64(time.Since(start).Seconds()))
			return ResponseWebhook{}, fmt.Errorf("Error parsing json. err=%s", err)
		} else {
			invocationHist.WithLabelValues("create", "success").Observe(float64(time.Since(start).Seconds()))
			return respData, nil
		}
	} else {
		logrus.Warnf("Webhook status != 202. resp=%s", resp)
		invocationHist.WithLabelValues("create", "error").Observe(float64(time.Since(start).Seconds()))
		return ResponseWebhook{}, fmt.Errorf("Failed to create backup. response")
	}
}

func deleteWebhookBackup(backupID string) error {
	logrus.Debugf("deleteWebhookBackup %s - waiting lock", backupID)
	webhookLock.Lock()
	defer webhookLock.Unlock()
	logrus.Debugf("deleteWebhookBackup %s - acquired lock", backupID)
	start := time.Now()
	resp, _, err := deleteHTTP(fmt.Sprintf("%s/%s", options.WebhookURL, backupID))
	if err != nil {
		logrus.Errorf("Webhook DELETE backup invocation failed. err=%s", err)
		invocationHist.WithLabelValues("delete", "error").Observe(float64(time.Since(start).Seconds()))
		return err
	}
	if resp.StatusCode == 200 {
		logrus.Debugf("Webhook DELETE successful")
		invocationHist.WithLabelValues("delete", "success").Observe(float64(time.Since(start).Seconds()))
		return nil
	} else if resp.StatusCode == 404 {
		logrus.Warnf("Webhook DELETE appears to be successful. Return was 404 NOT FOUND.")
		invocationHist.WithLabelValues("delete", "success").Observe(float64(time.Since(start).Seconds()))
		return nil
	} else {
		logrus.Warnf("Webhook status != 200. resp=%s", resp)
		invocationHist.WithLabelValues("delete", "error").Observe(float64(time.Since(start).Seconds()))
		return fmt.Errorf("Webhook status != 200. resp=%v", resp)
	}
}

func postHTTP(url string, data []byte) (http.Response, []byte, error) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("POST request=%v", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	logrus.Debugf("Response: %v", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}

func getHTTP(url0 string) (http.Response, []byte, error) {
	req, err := http.NewRequest("GET", url0, nil)
	if err != nil {
		logrus.Errorf("HTTP request creation failed. err=%s", err)
		return http.Response{}, []byte{}, err
	}

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	logrus.Debugf("GET request=%v", req)
	response, err1 := client.Do(req)
	if err1 != nil {
		logrus.Errorf("HTTP request invocation failed. err=%s", err1)
		return http.Response{}, []byte{}, err1
	}

	// logrus.Debugf("Response: %v", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)
	return *response, datar, nil
}
