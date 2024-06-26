import logging
import azure.functions as func
import pika
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
import pandas as pd
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.ensemble import IsolationForest
import numpy as np
import json
import uuid
from enum import Enum
import time
import random



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
    square_uuid = file_path.split('/')[1]
    
    connection_string = os.environ["AzureWebJobsStorage"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    logging.warning('bob service client: %s', blob_service_client.get_account_information())
    
    logging.warning('Downloading blob: %s', file_path)
    
    data = download_blob_to_file(blob_service_client, 'csv', file_path) 
    logging.warning('Blob downloaded: %s', file_path)
    
    cleaned_data = process_data(data)
    anomalies = detect_anomaly(cleaned_data)
    
    if anomalies.empty == False:
        logging.warning('Anomalies detected: %s', anomalies)
        anomaly_df = process_anomalies(anomalies, square_uuid=square_uuid)
        time.sleep(random.uniform(10, 500))
        send_anomaly_data_to_queue(anomaly_df)
    else:
        logging.warning('No anomalies detected')

def download_blob_to_file(blob_service_client: BlobServiceClient, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    csv_file = StringIO(blob_data.readall().decode('utf-8'))
    df = pd.read_csv(csv_file, index_col=0, parse_dates=True, dtype=float)
    return df


def process_anomalies(anomalies, square_uuid):
    anomaly_df = pd.DataFrame(columns=['location', 'dateTime', 'type', 'value'])

    # Create a list to store DataFrames for each anomaly
    anomaly_frames = []

    # Iterate over each timestamp/location and create a DataFrame for each anomaly
    for idx, row in anomalies.iterrows():
        anomaly_frames.append(pd.DataFrame({
            'location': [square_uuid] * len(row),
            'dateTime': [idx] * len(row),
            'type': row.index,
            'value': row.values
        }))

    # Concatenate the list of DataFrames into a single DataFrame
    anomaly_df = pd.concat(anomaly_frames, ignore_index=True)
    
    logging.warning('Anomalies Processed: %s', anomaly_df)
    return anomaly_df


# event header class
class EventCatalog(Enum):
    NEW_ANOMALY_DATA = 'NEW_ANOMALY_DATA'

class EventHeader:
    def __init__(self, eventID, eventCatalog):
        self.eventID = eventID
        self.eventCatalog = eventCatalog




def send_anomaly_data_to_queue(anomalies):
    connection_params = pika.ConnectionParameters(
        host=os.environ["RABBIT_MQ_HOST"],
        port=os.environ["RABBIT_MQ_PORT"],
        virtual_host=os.environ["RABBIT_MQ_VHOST"],
        credentials=pika.PlainCredentials(
            os.environ["RABBIT_MQ_USER"], os.environ["RABBIT_MQ_PASS"]
        ),
    )
    queue_name = "new_anomaly_queue"

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    logging.warning('Sending anomaly data to queue: %s', queue_name)
    
    # convert the dataframe to dict
    data_list = anomalies.to_dict(orient='records')
    
    # Convert 'dateTime' values to strings
    for record in data_list:
        record['dateTime'] = str(record['dateTime'])

    # create the event message
    event_header = EventHeader(uuid.uuid4(), EventCatalog.NEW_ANOMALY_DATA)
    event_message_dict = {
        "eventHeader": {
            "eventID": str(event_header.eventID),
            "eventCatalog": event_header.eventCatalog.value
        },
        "eventBody": f"{json.dumps(data_list)}"
    }
    # convert the event message to json
    json_payload = json.dumps(event_message_dict)
    channel.basic_publish(exchange='', routing_key=queue_name, body=json_payload)
    logging.warning('Anomaly data sent to queue: %s', queue_name)
    connection.close()

def process_data(df):
    logging.warning('Processing data: %s', df)
    # if there is a column with no values at all, it will be dropped
    df = df.dropna(axis=1, how='all')
    
    #  Using KnnImputer to fill the missing values
    # kni = KNNImputer(missing_values=np.nan, copy=False, add_indicator=True, weights='distance')
    kni = SimpleImputer(missing_values=np.nan, strategy='mean')
    df = pd.DataFrame(kni.fit_transform(df), index=df.index, columns=df.columns)    
    logging.warning('Data processed: %s', df)
    return df


def detect_anomaly(data):
    logging.warning('Detecting anomalies')    
    model=IsolationForest(n_estimators=100,max_samples='auto',random_state=42)
    model.fit(data)
    y_pred_outliers = model.predict(data)
    data['outlier'] = y_pred_outliers
    
    # Make sure 'outlier' is a column before filtering
    if 'outlier' in data.columns:
        # anomaly_columns = data.columns[data['outlier'] == -1].tolist()  # Convert to list
        return data[data['outlier'] == -1]
    else:
        logging.warning("No 'outlier' column found in the DataFrame.")
        return pd.DataFrame()  # Return an empty DataFrame if 'outlier' column is missing