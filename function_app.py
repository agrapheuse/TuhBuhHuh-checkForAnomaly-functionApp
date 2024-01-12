import logging
import azure.functions as func
import pika
import os
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
from sklearn.ensemble import IsolationForest
import numpy as np




app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="message", 
                                queue_name="agg-data-to-anomaly-detection",
                                connection="AzureWebJobsServiceBus") 
def checkForAnomaly(message: func.ServiceBusMessage) -> None:
    logging.info('Python ServiceBus Queue trigger processed a message: %s',
                message.get_body().decode('utf-8'))
    
    print("Service Bus message received")
    data = message.get_body().decode('utf-8')
    structured_data = process_data(data)
    detect_anomaly(structured_data)
    anomalies = detect_anomaly(structured_data)
    if anomalies:
        # If anomaly detected, send the anomaly data to another queue
        send_anomaly_data_to_queue(anomalies)


def send_anomaly_data_to_queue(anomalies):
    connection_params = pika.ConnectionParameters(
        host='localhost',
        port=5673,
        virtual_host='/',
        credentials=pika.PlainCredentials('myuser', 'mypassword')
    )

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    queue_name = 'new_data_queue'
    channel.queue_declare(queue=queue_name, durable=True)
    logging.info('Sending anomaly data to queue: %s', queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=anomalies)

    connection.close()

def process_data(data):
    df = pd.read_csv(StringIO(data))
    return df


def detect_anomaly(df):
    random_state = np.random.RandomState(42)
    model=IsolationForest(n_estimators=100,max_samples='auto',contamination=float(0.2),random_state=random_state)
    model.fit(df)
    print(model.get_params())
    y_pred_outliers = model.predict(df)
    print(y_pred_outliers)
    return y_pred_outliers