import logging
import azure.functions as func
import pika
import os
from azure.storage.blob import BlobServiceClient
from io import StringIO
import pandas as pd
from sklearn.ensemble import IsolationForest



@app.schedule(schedule="* */15 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False)
def checkForAnomaly(myTimer: func.TimerRequest) -> None:
    data = get_data_from_all_out_folder();
    anomaly = detect_anomaly(data)
    if anomaly:
        send_anomaly_data_to_queue(anomaly)
    if myTimer.past_due:
        logging.info('The timer is past due!')
        

    
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
        download_blob_to_file(blob_service_client, "csv", b.name)

    channel.basic_publish(exchange='', routing_key=queue_name, body="hello")
    logging.info(f"sent hello to queue {queue_name}")

    connection.close()

def get_data_from_all_out_folder():
    out_dir = "./all_out"
    codes = set()
    class Apis(str, Enum):
        OPEN_SENSE_MAP = "2889936e-8e2d-11ee-b9d1-0242ac120002"
        SENSOR_COMMUNITY = "017f12f5-8acb-4531-ab77-0e5208a31bca"
        TELRAAM = "8c9a8f25-e54e-4884-aee6-a4529c5424ba"

    dirs = os.listdir(out_dir)
    for dir in dirs:
        if dir == Apis.TELRAAM:
            for file in os.listdir(f"{out_dir}/{dir}"):
                with open(f"{out_dir}/{dir}/{file}") as f:
                    request = json.loads(f.read())
                    body = request["body"]
                    all_data = []
                    codes.add(body["status_code"])
                    for record in body["features"]:
                        data = {}
                        data["timesent"] = request["timeSent"]
                        data["segment_id"] = record["properties"]["segment_id"]
                        data["date"] = record["properties"]["date"]
                        data["uptime"] = record["properties"]["uptime"]
                        data["heavy"] = record["properties"]["heavy"]
                        data["car"] = record["properties"]["car"]
                        data["bike"] = record["properties"]["bike"]
                        data["pedestrian"] = record["properties"]["pedestrian"]
                        data["v85"] = record["properties"]["v85"]
                        all_data.append(data)
                    t_df = pd.concat(
                        [t_df, pd.DataFrame(all_data)],
                        ignore_index=True)
    telraam = copy.deepcopy(t_df)
    telraam = telraam.set_index(["timesent", "segment_id", "date"])
    telraam = telraam.drop_duplicates()
    return telraam


def get_data_from_blob(blob_service_client, folder):
    container_client = blob_service_client.get_container_client("csv")
    blob_list = container_client.list_blobs(name_starts_with=folder)
    merged_df = pd.DataFrame(columns=["squareUUID", "timestamp", "BIKE", "CAR", "HEAVY", "HUMIDITY", "PEDESTRIAN", "PM10", "PM25", "TEMPERATURE"])
    for blob in blob_list:
        df = download_blob_to_file(blob_service_client, "csv", blob.name)
        merged_df = merge_two_df(merged_df, df)
    merged_df = merged_df.drop(columns="squareUUID", axis=1)
    return merged_df

# using isolation foest detect anomalies from the data
def detect_anomaly(df):
    random_state = np.random.RandomState(42)
    model=IsolationForest(n_estimators=100,max_samples='auto',contamination=float(0.2),random_state=random_state)
    model.fit(df)
    print(model.get_params())
    y_pred_outliers = clf.predict(df)
    return y_pred_outliers



def download_blob_to_file(blob_service_client: BlobServiceClient, container_name, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = blob_client.download_blob()
    csv_file = StringIO(blob_data.readall().decode('utf-8'))
    df = pd.read_csv(csv_file)
    return df