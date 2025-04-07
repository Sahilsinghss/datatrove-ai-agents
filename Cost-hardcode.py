import boto3
import json
import datetime
from collections import defaultdict

# Use a specific AWS profile
# session = boto3.Session(profile_name="195275662048_AdministratorAccess",
#     region_name="us-east-1")


# Load service cost information from the JSON file
with open('costs.json') as f:
    service_costs = json.load(f)

# AWS Clients
# ec2_client = session.client('ec2')
# s3_client = session.client('s3')
# opensearch_client = session.client('opensearch')
# bedrock_client = session.client('bedrock')
# glue_client = session.client('glue')
# sns_client = session.client('sns')


ec2_client = boto3.client('ec2')
s3_client = boto3.client('s3')
opensearch_client = boto3.client('opensearch')
bedrock_client = boto3.client('bedrock')
glue_client = boto3.client('glue')
sns_client = boto3.client('sns')
# Define the cost per hour for different EC2 instance types
instances_cost_per_hour = {
    't3.small': 0.0208,   # Example cost per hour for t3.small
    'g4dn.xlarge': 0.526, # Example cost per hour for g4dn.xlarge
    't2.micro': 0.0116
}

# Function to get EC2 usage and cost for 8 hours per day
def get_ec2_cost():
    instances = ec2_client.describe_instances()
    cost = 0
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            instance_type = instance.get('InstanceType', '').strip().lower()  # Default to 't3.small'
            # Calculate the daily cost based on instance type
            running_hours = 8  # EC2 runs for 8 hours per day
            
            # Apply the cost based on instance type
            if instance_type in instances_cost_per_hour:
                # Calculate cost for 8 hours for this instance type
                cost += running_hours * instances_cost_per_hour[instance_type]
            else:
                print(f"Unknown instance type {instance_type}. No cost calculation applied.")
    
    return cost


# Function to get S3 usage
def get_s3_cost():
    buckets = s3_client.list_buckets()
    cost = 0
    for bucket in buckets['Buckets']:
        # Placeholder: calculate storage size, then cost
        bucket_size = 50  # Placeholder: you should fetch the actual storage size
        cost += bucket_size * service_costs.get('s3', 0)
    return cost

# Function to get OpenSearch usage and cost
# Define the cost of t3.small instance (this should be based on your pricing info)
instance_cost_per_hour = {
    't3.small': 0.036  # Example: replace with your actual cost per hour for t3.small
}

# Function to get OpenSearch usage and cost
def get_opensearch_cost():
    try:
        # Fetch all domain names
        domains = opensearch_client.list_domain_names()
        cost = 0
        
        # For each domain, we will calculate the cost based on the instance type
        for domain in domains['DomainNames']:
            domain_name = domain['DomainName']
            try:
                # Describe domain to get detailed information (e.g., instance type)
                domain_info = opensearch_client.describe_domain(DomainName=domain_name)
                
                # Check the structure of the domain info and access the correct field
                if 'DomainStatus' in domain_info:
                    # Assuming we have access to instance type information (can vary depending on your setup)
                    domain_status = domain_info['DomainStatus']
                    instance_type = domain_status.get('ElasticsearchClusterConfig', {}).get('InstanceType', 't3.small')  # Default to t3.small
                    
                    # Calculate cost based on the instance type
                    if instance_type in instance_cost_per_hour:
                        # Cost for 8 hours usage, adjust based on your pricing info
                        cost += instance_cost_per_hour[instance_type] * 24
                    else:
                        print(f"Unknown instance type {instance_type} for domain {domain_name}")
                else:
                    print(f"Warning: No DomainStatus found for domain {domain_name}")
            
            except KeyError as e:
                print(f"Error: Missing expected field {e} for domain {domain_name}")
            except Exception as e:
                print(f"Error fetching details for domain {domain_name}: {e}")
                continue
        
        return cost
    except Exception as e:
        print(f"Error fetching OpenSearch domain names: {e}")
        return 0

# Function to get Bedrock usage and cost
def get_bedrock_cost():
    # Placeholder: fetch usage data for Bedrock and calculate cost
    usage_hours = 1  # Placeholder: usage in hours
    cost = usage_hours * service_costs.get('bedrock', 0)
    return cost

# Function to get Glue usage and cost
def get_glue_cost():
    jobs = glue_client.get_jobs()
    cost = 0
    for job in jobs['Jobs']:
        # Placeholder: calculate DPU usage for Glue jobs
        job_dpu = 10  # Placeholder: use actual DPU usage
        cost += job_dpu * service_costs.get('glue', 0)
    return cost

# Function to generate suggestions based on total cost
def generate_suggestions(total_cost):
    suggestions = []
    if total_cost > 50:
        suggestions.append("Consider stopping unused EC2 instances.")
    if total_cost > 30:
        suggestions.append("Optimize S3 usage to save costs.")
    if total_cost > 100:
        suggestions.append("Consider using Reserved Instances for EC2 or Bedrock.")
    return suggestions

# Lambda Handler
def lambda_handler(event, context):
    total_cost = 0

    # Get costs for each service
    ec2_cost = get_ec2_cost()
    s3_cost = get_s3_cost()
    opensearch_cost = get_opensearch_cost()
    bedrock_cost = get_bedrock_cost()
    glue_cost = get_glue_cost()

    total_cost = ec2_cost + s3_cost + opensearch_cost + bedrock_cost + glue_cost

    # Generate suggestions based on total cost
    suggestions = generate_suggestions(total_cost)

    # Create the report message
    report = {
        "date": datetime.datetime.now().strftime('%Y-%m-%d'),
        "total_cost": total_cost,
        "ec2_cost": ec2_cost,
        "s3_cost": s3_cost,
        "opensearch_cost": opensearch_cost,
        "bedrock_cost": bedrock_cost,
        "glue_cost": glue_cost,
        "suggestions": suggestions
    }

    # Convert the report to JSON
    report_json = json.dumps(report)
    print(report_json)
    # Send SNS message
    sns_client.publish(
        TopicArn='arn:aws:sns:us-east-1:195275662048:FastAPIAlerts',  # Replace with your SNS topic ARN
        Message=report_json,
        Subject="Daily AWS Cost Estimation Report"
    )

    return {
        'statusCode': 200,
        'body': report_json
    }