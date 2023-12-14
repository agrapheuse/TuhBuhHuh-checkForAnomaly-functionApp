import logging
import azure.functions as func
import pika
import os
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd



@app.schedule(schedule="* */15 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False)
def checkForAnomaly(myTimer: func.TimerRequest) -> None:
    logging.info("Checking for anomalies in the data.")
    # Get the data from the database
    data = get_data()
    # Process the data and check for anomalies
    anomalies = process_data(data)
    if anomalies:
        logging.info("Anomalies found: ", anomalies)
    else:
        logging.info("No anomalies found.")

def process_data(data):
    """Processes the data and checks for anomalies.

    Args:
        data ([type]): [description]

    Returns:
        [type]: [description]
    """
    # Process the data and check for anomalies
    anomalies = []
    return anomalies


def get_blob_uploaded():
    """Gets the blob uploaded.

    Returns:
        [type]: [description]
    """
    # Get the connection string
    connection_string = os.getenv('AzureWebJobsStorage')
    # Create a blob service client
    blob_service_client = BlobServiceClient.from_connection_string(
        connection_string)
    # Get the blob container
    container_client = blob_service_client.get_container_client(
        'datacontainer')
    # Get the blob client
    blob_client = container_client.get_blob_client('data.csv')
    # Download the blob
    blob = blob_client.download_blob()
    # Read the blob
    blob_data = blob.readall()
    # Convert the blob to a string
    blob_string = str(blob_data, 'utf-8')
    # Create a string buffer
    data = StringIO(blob_string)
    # Create a dataframe
    df = pd.read_csv(data)
    # Return the dataframe
    return df