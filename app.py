import json
import os
import requests
import time
import datetime
from sys import stderr
from flask import Flask, request, jsonify

app = Flask(__name__)

api_key = os.environ.get("API_KEY", "")
if api_key == "":
    print("api key is required", file=stderr)

api_base_url = "https://api.stagingv3.microgen.id/query/api/v1/" + api_key

@app.route('/hdfs/metrics')
def hashtag():
    url = "http://10.10.65.1:8080/api/v1/clusters/sapujagad/services/HDFS/components/DATANODE"
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get('/hdfs/summary/datanodes')
def sumdatanodes():
    url = "http://10.10.65.1:8080/api/v1/clusters/sapujagad/hosts?fields=metrics/disk/disk_free,metrics/disk/disk_total,metrics/load/load_one&minimal_response=true"
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.get("/hdfs/summary/journalnodes")
def sumjournalnodes():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    e = int(time.mktime(time.strptime(d,p))) - 3600
    z = str(a)
    x = str(e)
    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/hosts?page_size=100&from=0&(host_components/HostRoles/component_name=JOURNALNODE)&minimal_response=true&_="+z+""
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
    return response.json()

@app.get("/hdfs/summary/nfsgateway")
def sumnfsgateway():
    d=str(datetime.datetime.now())
    p='%Y-%m-%d %H:%M:%S.%f'
    a = int(time.mktime(time.strptime(d,p)))
    e = int(time.mktime(time.strptime(d,p))) - 3600
    z = str(a)
    x = str(e)
    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/hosts?page_size=100&from=0&(host_components/HostRoles/component_name=NFS_GATEWAY)&minimal_response=true&_="+z+""
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
    return response.json()

@app.get("/hdfs/summary")
def summary():
    url = "http://10.10.65.1:8080/api/v1/clusters/sapujagad/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/hbase/master/IsActiveMaster,host_components/metrics/hbase/master/MasterStartTime,host_components/metrics/hbase/master/MasterActiveTime,host_components/metrics/hbase/master/AverageLoad,host_components/metrics/master/AssignmentManager/ritCount,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1667968440999"
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
#     x = str(response.json())
#     z = x.replace("'", '"' )
#     a = z.replace('{"items":', "")
#     b = a[:-1]
#     data = json.loads(b)
    return response.json()

@app.get("/filesystem")
def hashtags():
    source = str(request.args.get('source'))
    a = source.replace("/", "%2F")
    url="http://10.10.65.1:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/listdir?nameFilter=&path="+ a +"&_=1667963722829"
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
    return response.json()

@app.post("/hdfs/restart")
def restart():

    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/requests"
    username = "sapujagad"
    password = "kayangan"
    payload = '{"RequestInfo":{"context":"Execute REFRESH_NODES","command":"REFRESH_NODES"},"Requests/resource_filters":[{"service_name":"HDFS","component_name":"NAMENODE","hosts":"sapujagad-master01.kayangan.com"}]}'
    response = requests.post(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.put("/hdfs/stop")
def stop():

    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/services/HDFS"
    username = "sapujagad"
    password = "kayangan"
    payload = '{"RequestInfo":{"context":"_PARSE_.STOP.HDFS","operation_level":{"level":"SERVICE","cluster_name":"sapujagad","service_name":"HDFS"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
    response = requests.put(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.put("/hdfs/start")
def start():

    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/services/HDFS"
    username = "sapujagad"
    password = "kayangan"
    payload = '{"RequestInfo":{"context":"_PARSE_.START.HDFS","operation_level":{"level":"SERVICE","cluster_name":"sapujagad","service_name":"HDFS"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
    response = requests.put(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.get("/sqoop/summary")
def check():
    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name&minimal_response=true&_=1667968440999"
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))

    x = str(response.json())
    z = x.replace("'", '"' )
    a = z.replace('{"items":', "")
    b = a[:-1]
    data = json.loads(b)
    return jsonify(data)

@app.post("/mkdir")
def mkdir():
    url="http://10.10.65.1:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/mkdir"
    username = "sapujagad"
    password = "kayangan"
    content = request.json
    path = str(content['path'])
    # payload = '{"path" : "/user/sapujagad/test3"}'
    response = requests.put(url, auth=(username, password), json={"path": path})
    return response.json() 

@app.post("/sqoop/restart")
def srestart():
    url="http://10.10.65.1:8080/api/v1/clusters/sapujagad/requests"
    username = "sapujagad"
    password = "kayangan"
    payload = '{"RequestInfo":{"command":"RESTART","context":"Restart all components for SQOOP","operation_level":{"level":"SERVICE","cluster_name":"sapujagad","service_name":"SQOOP"}},"Requests/resource_filters":[{"service_name":"SQOOP","component_name":"SQOOP","hosts":"sapujagad-edge01.kayangan.com,sapujagad-edge02.kayangan.com,sapujagad-master01.kayangan.com,sapujagad-master02.kayangan.com,sapujagad-worker01.kayangan.com,sapujagad-worker02.kayangan.com,sapujagad-worker03.kayangan.com,sapujagad-worker04.kayangan.com,sapujagad-worker05.kayangan.com"}]}'
    response = requests.post(url, auth=(username, password), data=payload)
    # print(response.status_code)
    return response.json()

@app.delete("/hdfs/file")
def deletefile():
    url="http://10.10.65.1:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/moveToTrash"
    username = "sapujagad"
    password = "kayangan"
    content = request.json
    path = str(content['path'])
    recursive = "true"
    response = requests.post(url, auth=(username, password), json={"paths":[{"path": path,"recursive":recursive}]})
    # print(response.status_code)
    return response.json()


@app.delete("/hdfs/file/permanent")
def deletefilepermanent():
    url="http://10.10.65.1:8080/api/v1/views/FILES/versions/1.0.0/instances/hdfs_viewer/resources/files/fileops/remove"
    username = "sapujagad"
    password = "kayangan"
    content = request.json
    path = str(content['path'])
    recursive = "true"
    response = requests.post(url, auth=(username, password), json={"paths":[{"path": path,"recursive":recursive}]})
    # print(response.status_code)
    return response.json()

if __name__ == "__main__":
    app.run(debug=True)
