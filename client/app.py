import httpx
import backoff

class JobExecutor:
    def __init__(self, base_url, job_id):
        self.client = httpx.Client(base_url=base_url)
        self.job_id = job_id
        self.id = self.execute()
        self.running = self.poll()
        
    def get_result(self):
        response = self.client.get(f"/dw/data/v21_6/jobs/{self.job_id}/executions/{self.id}")
        content = response.json()
        return content

    def execute(self):
        response = self.client.post(f"/dw/data/v21_6/jobs/{self.job_id}/executions")
        content = response.json()
        print(f'{content["job_id"]} job started with execution id {content["id"]}')
        return content.get("id")

    @backoff.on_predicate(backoff.expo, base=2, factor=2, max_value=20)
    def poll(self):
        response = self.client.get(f"/dw/data/v21_6/jobs/{self.job_id}/executions/{self.id}")
        content = response.json()
        if content.get("status") == "RUNNING":
            print(f'{content["job_id"]} running with execution id {content["id"]} for {content["duration"]} seconds')
        else:
            return True



example_job_result = JobExecutor(base_url="http://127.0.0.1:8000", job_id="examplejob").get_result()
print(example_job_result)
