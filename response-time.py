import json
import psycopg2
import os
import pymongo
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression
from botocore.exceptions import ClientError
import boto3

SECRET_MANAGER = boto3.client("secretsmanager")


def get_pg_credentials():
    secret_name = os.environ.get("PG_SECRET_NAME")
    response = SECRET_MANAGER.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

def fetch_response_times(pg_creds):
    conn = psycopg2.connect(
        host=pg_creds["host"],
        port=pg_creds["port"],
        database=pg_creds["database"],
        user=pg_creds["username"],
        password=pg_creds["password"]
    )
    cursor = conn.cursor()
    cursor.execute("SELECT response_time FROM datatrove_api_logs ORDER BY timestamp DESC LIMIT 100;")
    data = [row[0] for row in cursor.fetchall()][::-1]
    cursor.close()
    conn.close()
    return data

def predict_response_time(times):
    X = np.arange(len(times)).reshape(-1, 1)
    y = np.array(times)
    model = LinearRegression().fit(X, y)
    return model.predict([[len(times)]])[0]

def store_prediction(pg_creds, user_name, prediction):
    conn = psycopg2.connect(
        host=pg_creds["host"],
        port=pg_creds["port"],
        database=pg_creds["database"],
        user=pg_creds["username"],
        password=pg_creds["password"]
    )
    cursor = conn.cursor()

    now = datetime.utcnow()
    print(now)
    insert_query = """
        INSERT INTO predict_response_times (user_name, timestamp, response_time)
        VALUES (%s, %s, %s)
    """
    cursor.execute(insert_query, (user_name, now, float(prediction)))
    conn.commit()

    cursor.close()
    conn.close()


def lambda_handler(event, context):

    pg_creds = get_pg_credentials()
    user_name = event.get("user", "sahil@nagarro.com")

    response_times = fetch_response_times(pg_creds)
    predicted = predict_response_time(response_times)
    store_prediction(pg_creds, user_name,predicted)
    print(predicted)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Prediction stored successfully",
            "predicted_response_time": predicted
        })
    }
