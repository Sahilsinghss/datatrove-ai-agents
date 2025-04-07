import json
import boto3
import time
import requests
import psycopg2
import gc
from datetime import datetime
import pytz


# AWS Clients
CLOUDWATCH = boto3.client("cloudwatch")
SSM_CLIENT = boto3.client("ssm")
SNS_CLIENT = boto3.client("sns")

# Global Variables
DB_CONFIG = {
    "host": "52.70.143.147",
    "port": "5432",
    "database": "datatrove_db",
    "user": "datatrove_user",
    "password": "ZHFoWB17M00S"
}

def log(message):
    """Append log with IST timestamp"""
    ist = pytz.timezone('Asia/Kolkata')
    timestamp = datetime.now(ist).strftime("[%Y-%m-%d %H:%M:%S]")
    logs.append(f"{timestamp} {message}")


def get_config():
    """Fetch configuration from database"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Use correct column names
    cursor.execute("SELECT param_name, param_value FROM monitoring_config;")
    config = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.close()
    conn.close()
    
    log("Fetched configuration from database")
    return config

SNS_SUBJECT_UTILIZATION = "🚨 AI Agent Alert: High Resource Utilization Detected!"
SNS_SUBJECT_HEALTH = "🚨 AI Agent Alert: FastAPI Health Check Failed!"
SNS_MESSAGE_HEALTH = """Hi Team,

The AI Agent detected an issue and is restarting the FastAPI service to restore functionality.

Regards,  
AI Monitoring System"""

def send_alert(subject, message):
    """Send alert using AWS SNS"""
    SNS_CLIENT.publish(
        TopicArn=config["SNS_TOPIC_ARN"],
        Subject=subject,
        Message=message
    )
    log(f"Alert Sent: {subject}")
    print(f"Alert Sent: {subject}")

def get_metric(metric_name, namespace, unit):
    """Get metric statistics from CloudWatch"""
    response = CLOUDWATCH.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=[{"Name": "InstanceId", "Value": config["INSTANCE_ID"]}],
        StartTime=time.time() - 300,
        EndTime=time.time(),
        Period=300,
        Statistics=["Average"],
        Unit=unit,
    )
    value = response["Datapoints"][-1]["Average"] if response["Datapoints"] else 0
    log(f"Fetched {metric_name}: {value}%")
    return value

def check_fastapi():
    """Check FastAPI health"""
    try:
        response = requests.get(config["FASTAPI_URL"], timeout=5)

        if response.status_code != 200:
            send_alert(SNS_SUBJECT_HEALTH, SNS_MESSAGE_HEALTH)
            log("FastAPI Unreachable! Restarting...")
            restart_fastapi()
            return

    except requests.exceptions.RequestException:
        print("FastAPI Unreachable!, Restarting...")
        log("FastAPI Unreachable! Restarting...")

def execute_command(command):
    """Execute command via AWS SSM"""
    response = SSM_CLIENT.send_command(
        InstanceIds=[config["INSTANCE_ID"]],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [command]},
    )
    log(f"Executed command: {command}")

def restart_fastapi():
    """Restart FastAPI using AWS SSM"""
    command = "sudo systemctl restart datatrove.service"

    response = SSM_CLIENT.send_command(
        InstanceIds=[config["INSTANCE_ID"]],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [command]},
    )

    command_id = response["Command"]["CommandId"]
    time.sleep(90)

    output = SSM_CLIENT.get_command_invocation(CommandId=command_id, InstanceId=config["INSTANCE_ID"])
    print("SSM Output:", output)
    log("FastAPI Restarted via SSM")

def clear_cache():
    execute_command("sync; echo 3 > /proc/sys/vm/drop_caches")
    log("Cache Cleared")

def restart_swap():
    execute_command("swapoff -a && swapon -a")
    log("Swap Restarted")

def kill_non_critical_process(resource_type):
    """Kill highest resource-consuming non-critical process"""
    process_kill_command = f"""
    ps aux --sort=-%{resource_type} | awk 'NR>1 && !/fastapi|python|sshd|systemd/ {{print $2, $11}}' | head -1 > /tmp/high_{resource_type}_process.log
    cat /tmp/high_{resource_type}_process.log
    awk '{{print $1}}' /tmp/high_{resource_type}_process.log | xargs -r kill -9
    """
    execute_command(process_kill_command)
    log(f"Killed high {resource_type} process")

def optimize_memory():
    gc.collect()
    print("Memory Optimized")

def lambda_handler(event, context):
    """Lambda function to check system health and send alerts"""
    global config, logs

    logs = []
    config = get_config()  # Fetch config at runtime
    check_fastapi()
    
    cpu_usage = event.get("cpu_usage", get_metric("CPUUtilization", "AWS/EC2", "Percent"))
    memory_usage = event.get("memory_usage", get_metric("mem_used_percent", "CWAgent", "Percent"))

    log(f"CPU Usage: {cpu_usage}%")
    log(f"Memory Usage: {memory_usage}%")

    print(cpu_usage, memory_usage)

    message_lines = ["Hi Team,", "The AI Agent has detected high resource utilization on the VM."]
    alert_triggered = False  

    if cpu_usage > int(config["CPU_THRESHOLD"]):
        message_lines.append(f"🔹 CPU Usage: {cpu_usage}% (Exceeded {config["CPU_THRESHOLD"]}%)")
        alert_triggered = True

    if memory_usage > int(config["MEMORY_THRESHOLD"]):
        message_lines.append(f"🔹 Memory Usage: {memory_usage}% (Exceeded {config["MEMORY_THRESHOLD"]}%)")
        alert_triggered = True

    if memory_usage > int(config["Normal_1"]):
        clear_cache()
    if memory_usage > int(config["Normal_2"]):
        optimize_memory()
    if memory_usage > int(config["Medium_1"]):
        restart_swap()
    if memory_usage > int(config["High_1"]):
        kill_non_critical_process("mem")
    if cpu_usage > int(config["High_1"]):
        kill_non_critical_process("cpu")
    if memory_usage > int(config["High_1"]) and cpu_usage > int(config["High_1"]):
        restart_fastapi()

    if alert_triggered:
        message_lines.append("\nImmediate attention is required.")
        message_lines.append("\nRegards,\nAI Monitoring System")

        send_alert(SNS_SUBJECT_UTILIZATION, "\n".join(message_lines))

    return {"statusCode": 200, "body": json.dumps({"message": "Health check completed!", "logs": logs})}
