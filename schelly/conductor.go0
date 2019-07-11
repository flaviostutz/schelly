package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
)

func launchWorkflow(scheduleName string) error {
	logrus.Debugf("startWorkflow scheduleName=%s", scheduleName)

	logrus.Debugf("Loading schedule definitions from DB")
	var schedule Schedule
	sc := mongoSession.Copy()
	defer sc.Close()
	st := sc.DB(dbName).C("schedules")

	err := st.Find(bson.M{"name": scheduleName}).One(&schedule)
	if err != nil {
		logrus.Errorf("Couldn't find schedule %s", scheduleName)
		return err
	}

	wf := make(map[string]interface{})
	wf["name"] = schedule.WorkflowName
	wf["version"] = schedule.WorkflowVersion
	wf["input"] = schedule.WorkflowContext
	wf["input"].(map[string]interface{})["scheduleName"] = schedule.Name
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

func findWorkflows(workflowType string, scheduleName string, running bool) (map[string]interface{}, error) {
	logrus.Debugf("findWorkflows %s", workflowType)
	runstr := ""
	if running {
		runstr = " AND status=RUNNING"
	} else {
		runstr = " AND NOT status=RUNNING"
	}
	freeText := fmt.Sprintf("workflowType:%s AND scheduleName=%s%s", workflowType, scheduleName, runstr)
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
