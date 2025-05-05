import boto3
import json
from datetime import datetime, timedelta

# Initialize AWS clients
ce_client = boto3.client('ce')
sns_client = boto3.client('sns')

# SNS Topic ARN (Replace with your actual ARN)
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:195275662048:FastAPIAlerts"

def get_daily_cost():
    ce_client = boto3.client('ce')

    # Get yesterday's date
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=1)

    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={'Start': start_date.strftime('%Y-%m-%d'), 'End': end_date.strftime('%Y-%m-%d')},
            Granularity='DAILY',
            Metrics=['BLENDED_COST', 'UNBLENDED_COST', 'NET_UNBLENDED_COST', 'AMORTIZED_COST'],
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
        )

        cost_data = {}
        for result in response.get('ResultsByTime', []):  # Ensure safe dictionary access
            for group in result.get('Groups', []):
                service = group['Keys'][0]
                try:
                    unblended_cost = float(group['Metrics']['UNBLENDED_COST']['Amount'])
                    net_unblended_cost = float(group['Metrics']['NET_UNBLENDED_COST']['Amount'])
                    blended_cost = float(group['Metrics']['BLENDED_COST']['Amount'])
                    amortized_cost = float(group['Metrics']['AMORTIZED_COST']['Amount'])
                except (KeyError, ValueError, TypeError) as e:
                    return {"error": f"Data processing error: {str(e)}"}

                cost_data[service] = {
                    'Original Cost (Before Credits)': net_unblended_cost,
                    'Cost After Credits': unblended_cost,
                    'Blended Cost': blended_cost,
                    'Amortized Cost': amortized_cost
                }

        return cost_data
    except Exception as e:
        return {"error": str(e)}  # Always return a dictionary



def suggest_cost_optimizations(cost_report):
    """Analyzes cost report and provides cost-saving suggestions."""
    
    suggestions = []
    
    if not isinstance(cost_report, dict):
        raise ValueError("Expected a dictionary for cost report")

    for service, cost_info in cost_report.items():
        # Convert cost values to float before comparison
        cost_after_credits = float(cost_info['CostAfterCredits'])
        cost_before_credits = float(cost_info['CostBeforeCredits'])

        if cost_after_credits > 10:  
            suggestions.append(f"Consider rightsizing {service}, cost is ${cost_after_credits:.2f}")
        elif cost_after_credits == 0:
            suggestions.append(f"Check if {service} is needed, as its cost is $0.")

    return suggestions

def send_sns_email(subject, message):
    """Sends cost report and suggestions via SNS."""
    
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )
    return response

def lambda_handler(event, context):
    """Main Lambda function entry point."""
    
    try:
        # Fetch daily cost report
        cost_report = get_daily_cost()

        # If an error occurred in get_daily_cost(), return immediately
        if "error" in cost_report:
            return {
                "statusCode": 500,
                "body": json.dumps({"error": cost_report["error"]})
            }

        # Generate cost-saving suggestions
        suggestions = suggest_cost_optimizations(cost_report)

        # Format email message
        report_message = "AWS Daily Cost Report:\n\n"
        for service, cost_info in cost_report.items():
            report_message += (
                f"- {service}:\n"
                f"  * Original Cost (Before Credits): ${cost_info['Original Cost (Before Credits)']:.2f}\n"
                f"  * Cost After Credits: ${cost_info['Cost After Credits']:.2f}\n"
                f"  * Blended Cost: ${cost_info['Blended Cost']:.2f}\n"
                f"  * Amortized Cost: ${cost_info['Amortized Cost']:.2f}\n\n"
            )

        report_message += "\nCost Optimization Suggestions:\n" + "\n".join(suggestions) if suggestions else "\nNo recommendations for today."

        # Send the report via SNS
        sns_response = send_sns_email("Daily AWS Cost Report", report_message)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Cost report sent successfully!",
                "snsResponse": sns_response
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

