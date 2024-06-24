<<<<<<< HEAD
# checkForAnomaly-functionApp
=======
# Check For Anomaly - Function App

Checks incoming data for anomalies and sends the detected anomalies to `Rabbitmq Queue`

## Retrieve message from Service Bus 

The app waits for the service bus queue to trigger and retrieves the attached message,

The message contains a url for the csv file stored in the blob storage.

## Retrieve csv file from blob storage

The app uses the afromentioned csv file path to query the blob service client and retrieve the data from the csv

The data is then converted to a dataframe.

## Process Data

The app then processes the data by 

- removing null columns
- filling null values using KNNImputer.


## Detect Anomalies 

Checks the processed data for anomalies using `Isolation Forest` and returns only rows where outliers are detected



## Process Anomalies

Anomalies are then converted into appropriate shape for transport 
| location    | dateTime | type       | value        |
|-------------|----------|------------|--------------|
| square_uuid | idx      | column name| column value|
| square_uuid | idx      | column name| column value|
| ...         | ...      | ...        | ...          |


## Send Anomalies To Rabbit MQ

Last Step is to send the anomalies to the rabbit mq queue
>>>>>>> upstream/main
