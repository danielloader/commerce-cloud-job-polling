from fastapi import FastAPI
from asyncio import sleep
from random import randint
from datetime import date, datetime, timedelta
from collections import defaultdict

app = FastAPI()

jobs = defaultdict(dict)


@app.get("/debug")
async def debug():
    return jobs


@app.post("/dw/data/v21_6/jobs/{job_id}/executions")
async def start_job_execution(job_id: str, id: str = str(randint(1000000, 9000000))):
    while id in jobs:
        id = str(randint(1000000, 9000000))
    jobs[job_id].update(
        {
            id: {
                "start": datetime.now(),
                "finish": datetime.now() + timedelta(seconds=randint(15, 45)),
            }
        }
    )
    return {
        "_v": "21.6",
        "_type": "job_execution",
        "_resource_state": "485715bae6023b28b849273a517f1f538d3c6290c70018e9afcaac767b9f513f",
        "client_id": "af7f5c90-ffc1-4ea4-9613-f5b375b7dc19",
        "duration": 0,
        "execution_status": "running",
        "id": str(id),
        "is_log_file_existing": False,
        "is_restart": False,
        "job_id": job_id,
        "log_file_name": "Job-JobExecutionTestJob1-20160520120033101.log",
        "modification_time": datetime.now().isoformat(),
        "start_time": datetime.now().isoformat(),
        "status": "RUNNING",
    }


@app.get("/dw/data/v21_6/jobs/{job_id}/executions/{id}")
async def get_job_execution(job_id: str, id: str):
    if id in jobs.get(job_id, {}):
        if jobs[job_id][id]["finish"] > datetime.now():
            return {
                "_v": "21.6",
                "_type": "job_execution",
                "_resource_state": "485715bae6023b28b849273a517f1f538d3c6290c70018e9afcaac767b9f513f",
                "client_id": "af7f5c90-ffc1-4ea4-9613-f5b375b7dc19",
                "duration": int(
                    (datetime.now() - jobs[job_id][id]["start"]).total_seconds()
                ),
                "execution_status": "running",
                "id": str(id),
                "is_log_file_existing": False,
                "is_restart": False,
                "job_id": job_id,
                "log_file_name": "Job-JobExecutionTestJob1-20160520120033101.log",
                "modification_time": jobs[job_id][id]["start"],
                "start_time": jobs[job_id][id]["start"],
                "status": "RUNNING",
            }
        else:
            return {
                "_v": "21.6",
                "_type": "job_execution",
                "_resource_state": "ac83c2e1ff3129ed6579c5fa95f746047d7db6867dd26da78dc89d23083127a3",
                "client_id": "af7f5c90-ffc1-4ea4-9613-f5b375b7dc19",
                "duration": (jobs[job_id][id]["finish"]-jobs[job_id][id]["start"]).total_seconds(),
                "end_time": jobs[job_id][id]["finish"].isoformat(),
                "execution_scopes": ["SiteGenesis", "SiteGenesis-US", "SiteGenesis-DE"],
                "execution_status": "finished",
                "exit_status": {"_type": "status", "code": "OK", "status": "ok"},
                "id": str(id),
                "is_log_file_existing": True,
                "is_restart": False,
                "job_id": job_id,
                "log_file_name": "Job-JobExecutionTestJob1-20160520115446018.log",
                "modification_time": "2016-05-20T11:54:46.224Z",
                "start_time": jobs[job_id][id]["start"].isoformat(),
                "status": "OK",
                "step_executions": [
                    {
                        "_type": "job_step_execution",
                        "duration": 16,
                        "end_time": "2016-05-20T11:54:46.075Z",
                        "execution_scope": "Organization",
                        "execution_status": "finished",
                        "exit_status": {
                            "_type": "status",
                            "code": "OK",
                            "message": "",
                            "status": "ok",
                        },
                        "id": "41265",
                        "modification_time": "2016-05-20T11:54:46.075Z",
                        "start_time": "2016-05-20T11:54:46.059Z",
                        "status": "OK",
                        "step_description": "A step that executes a pipeline that can be configured via a parameter",
                        "step_id": "Step1",
                        "step_type_id": "ExecutePipeline",
                        "step_type_info": "TestScheduleJobProcessPipeline-StartEnd",
                    },
                    {
                        "_type": "job_step_execution",
                        "duration": 12,
                        "end_time": "2016-05-20T11:54:46.102Z",
                        "execution_scope": "Organization",
                        "execution_status": "finished",
                        "exit_status": {
                            "_type": "status",
                            "code": "OK",
                            "message": "",
                            "status": "ok",
                        },
                        "id": "41266",
                        "modification_time": "2016-05-20T11:54:46.103Z",
                        "start_time": "2016-05-20T11:54:46.090Z",
                        "status": "OK",
                        "step_description": "A step that executes a script module function that can be configured via a parameter",
                        "step_id": "Step2",
                        "step_type_id": "ExecuteScriptModule",
                        "step_type_info": "testScriptModuleStep.ds#execute",
                    },
                    {
                        "_type": "job_step_execution",
                        "duration": 16,
                        "end_time": "2016-05-20T11:54:46.132Z",
                        "execution_scope": "SiteGenesis-DE",
                        "execution_status": "finished",
                        "exit_status": {
                            "_type": "status",
                            "code": "OK",
                            "message": "",
                            "status": "ok",
                        },
                        "id": "41267",
                        "include_steps_from_job_id": "JobExecutionTestJob2",
                        "modification_time": "2016-05-20T11:54:46.132Z",
                        "start_time": "2016-05-20T11:54:46.116Z",
                        "status": "OK",
                        "step_description": "A step that executes a pipeline that can be configured via a parameter",
                        "step_id": "Step3 - Step1",
                        "step_type_id": "ExecutePipeline",
                        "step_type_info": "TestScheduleJobProcessPipeline-StartEnd",
                    },
                    {
                        "_type": "job_step_execution",
                        "duration": 14,
                        "end_time": "2016-05-20T11:54:46.159Z",
                        "execution_scope": "SiteGenesis-DE",
                        "execution_status": "finished",
                        "exit_status": {
                            "_type": "status",
                            "code": "SUCCESS",
                            "message": "Step was successful!",
                            "status": "ok",
                        },
                        "id": "41268",
                        "include_steps_from_job_id": "JobExecutionTestJob2",
                        "modification_time": "2016-05-20T11:54:46.159Z",
                        "start_time": "2016-05-20T11:54:46.145Z",
                        "status": "SUCCESS",
                        "step_description": "",
                        "step_id": "Step3 - Step2",
                        "step_type_id": "ExecuteScriptModule",
                        "step_type_info": "testScriptModuleStep.ds#testReturnStatusOkWithCodeAndMessage",
                    },
                    {
                        "_type": "job_step_execution",
                        "duration": 15,
                        "end_time": "2016-05-20T11:54:46.189Z",
                        "execution_scope": "SiteGenesis",
                        "execution_status": "finished",
                        "exit_status": {
                            "_type": "status",
                            "code": "OK",
                            "message": "",
                            "status": "ok",
                        },
                        "id": "41269",
                        "include_steps_from_job_id": "JobExecutionTestJob2",
                        "modification_time": "2016-05-20T11:54:46.189Z",
                        "start_time": "2016-05-20T11:54:46.174Z",
                        "status": "OK",
                        "step_description": "A step that executes a pipeline that can be configured via a parameter",
                        "step_id": "Step3 - Step1",
                        "step_type_id": "ExecutePipeline",
                        "step_type_info": "TestScheduleJobProcessPipeline-StartEnd",
                    },
                    {
                        "_type": "job_step_execution",
                        "duration": 13,
                        "end_time": "2016-05-20T11:54:46.216Z",
                        "execution_scope": "SiteGenesis",
                        "execution_status": "finished",
                        "exit_status": {
                            "_type": "status",
                            "code": "SUCCESS",
                            "message": "Step was successful!",
                            "status": "ok",
                        },
                        "id": "41270",
                        "include_steps_from_job_id": "JobExecutionTestJob2",
                        "modification_time": "2016-05-20T11:54:46.216Z",
                        "start_time": "2016-05-20T11:54:46.203Z",
                        "status": "SUCCESS",
                        "step_description": "",
                        "step_id": "Step3 - Step2",
                        "step_type_id": "ExecuteScriptModule",
                        "step_type_info": "testScriptModuleStep.ds#testReturnStatusOkWithCodeAndMessage",
                    },
                ],
            }
    else:
        return {
            "_v": "21.6",
            "fault": {
                "arguments": {"executionId": id},
                "type": "JobExecutionNotFoundException",
                "message": f"No Job execution with ID '{id}' was found.",
            },
        }
