import os
import json
import boto3
from datetime import datetime, timedelta
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_aws import ChatBedrock

# ==== Environment Variables ====
REGION = "us-east-1"
FASTAPI_INSTANCE_ID = os.environ["FASTAPI_INSTANCE_ID"]
DB_INSTANCE_ID = os.environ["DB_INSTANCE_ID"]
LOG_GROUP = os.environ["LOG_GROUP"]
SNS_TOPIC = os.environ["SNS_TOPIC"]
FASTAPI_SERVICE_NAME = "fastapi.service"
DB_SERVICE_NAME = "db.service"
PORT_TO_FREE = "8000"

# ==== AWS Clients ====
ssm = boto3.client("ssm", region_name=REGION)
logs_client = boto3.client("logs", region_name=REGION)
sns_client = boto3.client("sns", region_name=REGION)

# ==== Prompt Template ====
PROMPT_TEMPLATE = """<|begin_of_text|><|start_header_id|>user<|end_header_id|>
You are an AI Root Cause Analyzer and Auto-Fix Agent.

1. Analyze the logs and describe the root cause.
2. Identify the impacted component (e.g. FastAPI, Database, OS, Nginx, Disk Space, Network, OS).
3. Suggest a structured fix in JSON format using the following schema:

{{
  "issue": "...",
  "component": "...",
  "fix": {{
    "action": "...",
    "target": "...",
    "service": "...",
    "package": "...",
    "port": ...,
    "directory": "..."
  }}
}}

Instructions:
- Ignore routine 404 logs like: `X.X.X.X:PORT - "GET / HTTP/1.1" 404`
- Ignore logs from the `/sse` route
- Do not include these in the analysis or fix suggestions

Logs:
{log_text}
<|eot_id|>
"""


def initialize_llm():
    return ChatBedrock(
        model_id="arn:aws:bedrock:us-east-1:195275662048:inference-profile/us.meta.llama3-1-8b-instruct-v1:0",
        provider="meta",  # Required when using ARN
        region="us-east-1",
        model_kwargs=dict(
            temperature=0.3,
            max_tokens=1024,
            top_p=0.9
        )
    )

llm_prompt = PromptTemplate.from_template(PROMPT_TEMPLATE)
llm = initialize_llm()
llm_chain = LLMChain(prompt=llm_prompt, llm=llm)

# ==== Lambda Handler ====
def lambda_handler(event, context):
    try:
        log_group = LOG_GROUP
        minutes = event.get("time_range_minutes", 5)
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(minutes=minutes)).timestamp() * 1000)

        logs = fetch_logs(log_group, start_time, end_time)
        if not logs:
            print("No logs found.")
            return {"status": "no_logs_found"}

        log_text = "\n".join([entry["message"] for entry in logs])
        print("Logs Fetched. Sending to LLM...")

        llm_response = llm_chain.run(log_text=log_text)
        print("LLM Output:", llm_response)

        send_alert("AI Root Cause Analysis (Raw Text)", llm_response)

        fix_target, fix_command = parse_fix_and_prepare_command(llm_response)
        print(fix_target, fix_command)
        if fix_target and fix_command:
            send_ssm_command(fix_target, fix_command)

        return {"status": "success"}

    except Exception as e:
        print(f"Unhandled error: {e}")
        return {"status": "error", "details": str(e)}


# ==== Helper Functions ====

def fetch_logs(log_group_name, start_time, end_time):
    try:
        response = logs_client.filter_log_events(
            logGroupName=log_group_name,
            startTime=start_time,
            endTime=end_time,
            limit=1000
        )
        return response.get("events", [])
    except Exception as e:
        print(f"Error fetching logs: {e}")
        return []

def send_alert(subject, message):
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject,
            Message=message
        )
    except Exception as e:
        print(f"Error sending SNS alert: {e}")

def parse_fix_and_prepare_command(llm_output):
    try:
        data = json.loads(llm_output)
        fix = data.get("fix", {})
        action = fix.get("action")
        target = fix.get("target")

        if target == "fastapi-instance":
            instance_id = FASTAPI_INSTANCE_ID
        elif target == "db-instance":
            instance_id = DB_INSTANCE_ID
        else:
            return None, None

        if action == "restart_service":
            return instance_id, f"sudo systemctl restart {fix['service']}"
        elif action == "install_package":
            return instance_id, f"pip install {fix['package']}"
        elif action == "kill_process":
            return instance_id, f"sudo fuser -k {fix['port']}/tcp"
        elif action == "clean_tmp":
            return instance_id, f"sudo find {fix['directory']} -type f -mtime +1 -delete"
        elif action == "reboot":
            return instance_id, "sudo reboot"
        else:
            return None, None
    except Exception as e:
        print("Failed to parse LLM output:", e)
        return None, None

def send_ssm_command(instance_id, command):
    try:
        print(f"Sending command to {instance_id}: {command}")
        ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]},
        )
    except Exception as e:
        print(f"Error sending SSM command: {e}")
