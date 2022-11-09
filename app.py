import json
import os
import requests
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

@app.get("/hdfs/summary")
def summary():
    url = "http://10.10.65.1:8080/api/v1/clusters/sapujagad/components/?ServiceComponentInfo/component_name=APP_TIMELINE_SERVER|ServiceComponentInfo/category.in(MASTER,CLIENT)&fields=ServiceComponentInfo/service_name,host_components/HostRoles/display_name,host_components/HostRoles/host_name,host_components/HostRoles/public_host_name,host_components/HostRoles/state,host_components/HostRoles/maintenance_state,host_components/HostRoles/stale_configs,host_components/HostRoles/ha_state,host_components/HostRoles/desired_admin_state,,host_components/metrics/jvm/memHeapUsedM,host_components/metrics/jvm/HeapMemoryMax,host_components/metrics/jvm/HeapMemoryUsed,host_components/metrics/jvm/memHeapCommittedM,host_components/metrics/mapred/jobtracker/trackers_decommissioned,host_components/metrics/cpu/cpu_wio,host_components/metrics/rpc/client/RpcQueueTime_avg_time,host_components/metrics/dfs/FSNamesystem/*,host_components/metrics/dfs/namenode/Version,host_components/metrics/dfs/namenode/LiveNodes,host_components/metrics/dfs/namenode/DeadNodes,host_components/metrics/dfs/namenode/DecomNodes,host_components/metrics/dfs/namenode/TotalFiles,host_components/metrics/dfs/namenode/UpgradeFinalized,host_components/metrics/dfs/namenode/Safemode,host_components/metrics/runtime/StartTime,host_components/metrics/hbase/master/IsActiveMaster,host_components/metrics/hbase/master/MasterStartTime,host_components/metrics/hbase/master/MasterActiveTime,host_components/metrics/hbase/master/AverageLoad,host_components/metrics/master/AssignmentManager/ritCount,host_components/metrics/dfs/namenode/ClusterId,host_components/metrics/yarn/Queue,host_components/metrics/yarn/ClusterMetrics/NumActiveNMs,host_components/metrics/yarn/ClusterMetrics/NumLostNMs,host_components/metrics/yarn/ClusterMetrics/NumUnhealthyNMs,host_components/metrics/yarn/ClusterMetrics/NumRebootedNMs,host_components/metrics/yarn/ClusterMetrics/NumDecommissionedNMs&minimal_response=true&_=1667968440999"
    username = "sapujagad"
    password = "kayangan"
    response = requests.get(url, auth=(username, password))
    # print(response.status_code)
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

@app.get("/products/<id>")
def getProductById(id):
    try:
        url = "/".join([api_base_url, "products", id])
        response = requests.get(url)
        respBody = response.json()

        if response.status_code != 200:
            if respBody.get('message') == 'project not found':
                respJson = jsonify(
                    {"message": "failed to connect to your project, please check if the api had been set properly."}, 
                )
                respJson.status_code = response.status_code

                return respJson

            respJson = jsonify(respBody)
            respJson.status_code = response.status_code

            return respJson

        return jsonify(respBody)
    except Exception as e:
        return jsonify({"message": "error occured: " + e.__str__()})

@app.patch("/products/<id>")
def updateProduct(id):
    try:
        url = "/".join([api_base_url, "products", id])
        response = requests.patch(url, json.dumps(request.json, indent=2))
        respBody = response.json()

        if response.status_code != 200:
            if respBody.get('message') == 'project not found':
                respJson = jsonify(
                    {"message": "failed to connect to your project, please check if the api had been set properly."}, 
                )
                respJson.status_code = response.status_code

                return respJson

            respJson = jsonify(respBody)
            respJson.status_code = response.status_code

            return respJson
        
        return jsonify(respBody)
    except Exception as e:
        return jsonify({"message": "error occured: " + e.__str__()})

@app.delete("/products/<id>")
def deleteProduct(id):
    try:
        url = "/".join([api_base_url, "products", id])
        response = requests.delete(url)
        respBody = response.json()

        if response.status_code != 200:
            if respBody.get('message') == 'project not found':
                respJson = jsonify(
                    {"message": "failed to connect to your project, please check if the api had been set properly."}, 
                )
                respJson.status_code = response.status_code

                return respJson

            respJson = jsonify(respBody)
            respJson.status_code = response.status_code

            return respJson

        return jsonify(respBody)
    except Exception as e:
        return jsonify({"message": "error occured: " + e.__str__()})

if __name__ == "__main__":
    app.run(debug=True)
