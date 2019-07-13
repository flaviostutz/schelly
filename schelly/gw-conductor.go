package schelly

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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

type WorkflowInstance struct {
	workflowID string
	status     string
	dataID     *string
	dataSizeMB *float64
	startTime  time.Time
	endTime    time.Time
}

var WORKFLOW_CREATE = "create-backup"
var WORKFLOW_REMOVE = "remove-backup"

func InitConductor() {
	prometheus.MustRegister(invocationHist)
}

func launchCreateBackupWorkflow(backupName string) (workflowID string, err error) {
	logrus.Debugf("startWorkflow backupName=%s", backupName)

	logrus.Debugf("Loading backup definition from DB")
	bs, err := getBackupSpec(backupName)
	if err != nil {
		return "", err
	}

	if bs.Enabled == 0 {
		return "", fmt.Errorf("Backup %s cannot be launched because it is not enabled", bs.Name)
	}

	wf := make(map[string]interface{})
	wf["name"] = WORKFLOW_CREATE
	// wf["version"] = "1.0"
	mi := make(map[string]interface{})
	mi["backupName"] = bs.Name
	wf["input"] = mi
	wfb, _ := json.Marshal(wf)

	logrus.Debugf("Launching Workflow %s", wf)
	url := fmt.Sprintf("%s/workflow", opt.ConductorAPIURL)
	resp, data, err := postHTTP(url, wfb, "backup_create")
	if err != nil {
		logrus.Errorf("Call to Conductor POST /workflow failed. err=%s", err)
		return "", err
	}
	if resp.StatusCode != 200 {
		logrus.Warnf("POST /workflow call status!=200. resp=%v", resp)
		return "", fmt.Errorf("Failed to create new workflow instance. status=%d", resp.StatusCode)
	}
	logrus.Infof("Workflow %s launched for creating backup %s. workflowId=%s", WORKFLOW_CREATE, backupName, string(data))
	return string(data), nil
}

func launchRemoveBackupWorkflow(dataID string) (workflowID string, err error) {
	logrus.Debugf("removeBackupWorkflow dataID=%s", dataID)

	wf := make(map[string]interface{})
	wf["name"] = WORKFLOW_REMOVE
	// wf["version"] = "1.0"
	mi := make(map[string]interface{})
	mi["dataID"] = dataID
	wf["input"] = mi
	wfb, _ := json.Marshal(wf)

	logrus.Debugf("Launching Workflow %s", wf)
	url := fmt.Sprintf("%s/workflow", opt.ConductorAPIURL)
	resp, data, err := postHTTP(url, wfb, "backup_remove")
	if err != nil {
		logrus.Errorf("Call to Conductor POST /workflow failed. err=%s", err)
		return "", err
	}
	if resp.StatusCode != 200 {
		logrus.Warnf("POST /workflow call status!=200. resp=%v", resp)
		return "", fmt.Errorf("Failed to create new workflow instance. status=%d", resp.StatusCode)
	}

	workflowID = string(data)
	logrus.Infof("Workflow %s launched for removing dataID %s. workflowId=%s", WORKFLOW_REMOVE, dataID, workflowID)

	return workflowID, nil
}

func getWorkflowInstance(workflowID string) (WorkflowInstance, error) {
	logrus.Debugf("getWorkflowInstance %s", workflowID)
	wi := WorkflowInstance{}
	resp, data, err := getHTTP(fmt.Sprintf("%s/workflow/%s?includeTasks=false", opt.ConductorAPIURL, workflowID), "get_workflow")
	if err != nil {
		return wi, fmt.Errorf("GET /workflow/%s?includeTasks=false failed. err=%s", err, workflowID)
	}
	if resp.StatusCode != 200 {
		return wi, fmt.Errorf("Couldn't get workflow info. workflowId=%s. status=%d", workflowID, resp.StatusCode)
	}

	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return WorkflowInstance{}, err
	}
	wi.workflowID = wfdata["id"].(string)
	wi.status = wfdata["status"].(string)
	out, exists := wfdata["output"]
	if exists {
		wfoutput := out.(map[string]interface{})
		did, ex := wfoutput["dataId"]
		if ex {
			a := did.(string)
			wi.dataID = &a
		}
	}
	//FIXME
	wi.startTime = wfdata["startTime"].(time.Time)
	wi.endTime = wfdata["endTime"].(time.Time)
	return wi, nil
}

func findWorkflows(backupName string, running bool) (hits int, result map[string]interface{}, err error) {
	logrus.Debugf("findWorkflows %s", backupName)
	runstr := ""
	if running {
		runstr = " AND status=RUNNING"
	} else {
		runstr = " AND NOT status=RUNNING"
	}
	freeText := fmt.Sprintf("backupName=%s%s", backupName, runstr)
	sr := fmt.Sprintf("%s/workflow/search?freeText=%s&sort=endTime:DESC&size=5", opt.ConductorAPIURL, url.QueryEscape(freeText))
	// logrus.Debugf("WORKFLOW SEARCH URL=%s", sr)
	resp, data, err := getHTTP(sr, "list_workflows")
	if err != nil {
		return 0, nil, fmt.Errorf("GET /workflow/search failed. err=%s", err)
	}
	if resp.StatusCode != 200 {
		return 0, nil, fmt.Errorf("GET /workflow/search failed. status=%d. err=%s", resp.StatusCode, err)
	}
	var wfdata map[string]interface{}
	err = json.Unmarshal(data, &wfdata)
	if err != nil {
		logrus.Errorf("Error parsing json. err=%s", err)
		return 0, nil, err
	}
	hits = int(wfdata["totalHits"].(float64))
	return hits, wfdata, nil
}

func postHTTP(url string, data []byte, metricsInfo string) (http.Response, []byte, error) {
	startTime := time.Now()
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

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		logrus.Debugf("%s status code not ok. status_code=%d", metricsInfo, response.StatusCode)
	}
	invocationHist.WithLabelValues(metricsInfo, fmt.Sprintf("%d", response.StatusCode)).Observe(float64(time.Since(startTime).Seconds()))

	return *response, datar, nil
}

func getHTTP(url0 string, metricsInfo string) (http.Response, []byte, error) {
	startTime := time.Now()
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
		invocationHist.WithLabelValues(metricsInfo, "error").Observe(float64(time.Since(startTime).Seconds()))
		return http.Response{}, []byte{}, err1
	}

	// logrus.Debugf("Response: %v", response)
	datar, _ := ioutil.ReadAll(response.Body)
	logrus.Debugf("Response body: %s", datar)

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		logrus.Debugf("%s status code not ok. status_code=%d", metricsInfo, response.StatusCode)
	}
	invocationHist.WithLabelValues(metricsInfo, fmt.Sprintf("%d", response.StatusCode)).Observe(float64(time.Since(startTime).Seconds()))

	return *response, datar, nil
}
