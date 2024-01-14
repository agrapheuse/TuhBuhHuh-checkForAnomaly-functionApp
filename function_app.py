import logging
import azure.functions as func
import pika
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.ensemble import IsolationForest
import numpy as np


app = func.FunctionApp()


@app.service_bus_queue_trigger(arg_name="message", 
                                queue_name="agg-data-to-anomaly-detection",
                                connection="AzureWebJobsServiceBus") 
def main(message: func.ServiceBusMessage) -> None:
    logging.warning('Python ServiceBus Queue trigger processed a message: %s',
                message.get_body().decode('utf-8'))
    
    file_path = message.get_body().decode('utf-8')
    
    # remove the first '/history' from the path
    file_path = file_path.replace("/history", "", 1)
    
    connection_string = "DefaultEndpointsProtocol=https;AccountName=datalaketuhbehhuh;AccountKey=C2te9RgBRHhIH8u3tydAsn9wNd4umdD2axq1ZdcfKh7CZRpL04+D4H6QinE/gckMTUA/dFj1kFpd+ASt4+/8ZA==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    logging.warning('bob service client: %s', blob_service_client.get_account_information())
    
    logging.warning('Downloading blob: %s', file_path)
    
    data = download_blob_to_file(blob_service_client, 'csv', file_path) 
    logging.warning('Blob downloaded: %s', file_path)
    
    original_data = data.copy()
    structured_data = process_data(data, original_data)
    anomalies = detect_anomaly(structured_data,original_data)
    logging.warning('Anomalies detected: %s', anomalies)
    send_anomaly_data_to_queue(anomalies)

def download_blob_to_file(blob_service_client: BlobServiceClient, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    csv_file = StringIO(blob_data.readall().decode('utf-8'))
    df = pd.read_csv(csv_file, index_col=0, parse_dates=True, dtype=float)
    return df



def send_anomaly_data_to_queue(anomalies):
    connection_params = pika.ConnectionParameters(
        host=os.environ['RABBITMQ_HOST'],
        port=os.environ['RABBITMQ_PORT'],
        virtual_host=['RABBITMQ_VHOST'],
        credentials=pika.PlainCredentials(os.environ['RABBITMQ_USER'], os.environ['RABBITMQ_PASS'])
    )
    queue_name = "new_anomaly_event"

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    logging.info('Sending anomaly data to queue: %s', queue_name)
    channel.basic_publish(exchange='', routing_key=queue_name, body=anomalies)

    connection.close()

def process_data(knn_imputer,df, original_data):
    logging.warning('Processing data: %s', df)
    # if there is a column with no values at all, it will be dropped
    df = df.dropna(axis=1, how='all')
    
    #  Using KnnImputer to fill the missing values
    kni = KNNImputer(missing_values=np.nan, copy=False, add_indicator=True, weights='distance')
    df = pd.DataFrame(df=kni.fit_transform(df), index=original_data.index)
    
    # Put the deleted/empty columns back.
    original_data = original_data.drop(df.columns, axis=1)
    df = pd.concat([df, original_data], axis=1)
    
    logging.warning('Data processed: %s', df)
    return df


def detect_anomaly(data, original_data):
    logging.warning('Detecting anomalies')
    # if there is a column with no values at all, it will be dropped
    data = data.dropna(axis=1, how='all')
    model=IsolationForest(n_estimators=100,max_samples='auto',contamination=float(0.2),random_state=42)
    model.fit(data)
    y_pred_outliers = model.predict(data)
    data['anomaly_score'] = model.decision_function(data)
    data['outlier'] = y_pred_outliers
    
    # Put the deleted/empty columns back.
    original_data = original_data.drop(data.columns, axis=1)
    data = pd.concat([original_data, data], axis=1)  
    
    # only return rows where  outlier is -1
    data = data[data['outlier']==-1]
    return data