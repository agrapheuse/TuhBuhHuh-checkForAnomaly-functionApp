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
    
    print("timer triggered")
    connection_string = "DefaultEndpointsProtocol=https;AccountName=datalaketuhbehhuh;AccountKey=C2te9RgBRHhIH8u3tydAsn9wNd4umdD2axq1ZdcfKh7CZRpL04+D4H6QinE/gckMTUA/dFj1kFpd+ASt4+/8ZA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    print("blob service client created")
    print(blob_service_client.account_name)
    data= get_data_from_blob(blob_service_client, "csv")
    print(data)
    detect_anomaly(data)
    anomaly = detect_anomaly(data)
    if anomaly:
        send_anomaly_data_to_queue(anomaly)


def send_anomaly_data_to_queue():
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
    
    connection_string = os.environ["MyStorageAccountConnection"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("csv")
    blobs = container_client.list_blobs()
    for b in blobs:
        print(b.name)
        download_blob(blob_service_client, "csv", b.name)

    channel.basic_publish(exchange='', routing_key=queue_name, body="hello")
    logging.info(f"sent hello to queue {queue_name}")

    connection.close()


def get_data_from_blob(blob_service_client, folder):
    container_client = blob_service_client.get_container_client("csv")
    blob_list = container_client.list_blobs(name_starts_with=folder)
    
    for blob in blob_list:
        df = download_blob(blob_service_client, "csv", blob.name)
        merged_df = merge_two_df(merged_df, df)
        
    merged_df = pd.DataFrame(columns=["squareUUID", "timestamp", "BIKE", "CAR", "HEAVY", "HUMIDITY", "PEDESTRIAN", "PM10", "PM25", "TEMPERATURE"])
    
    merged_df = merged_df.drop(columns="squareUUID", axis=1)
    return merged_df

def merge_two_df(df1, df2):
    appended_df = pd.concat([df1, df2], ignore_index=True)
    return appended_df


def detect_anomaly(df):
    random_state = np.random.RandomState(42)
    model=IsolationForest(n_estimators=100,max_samples='auto',contamination=float(0.2),random_state=random_state)
    model.fit(df)
    print(model.get_params())
    y_pred_outliers = model.predict(df)
    print(y_pred_outliers)
    return y_pred_outliers



def download_blob(blob_service_client: BlobServiceClient, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    csv_file = StringIO(blob_data.readall().decode('utf-8'))
    df = pd.read_csv(csv_file)
    return df