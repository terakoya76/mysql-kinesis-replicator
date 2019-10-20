import base64
import json

import boto3
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.constants import BINLOG

mysql_settings = {"host": "127.0.0.1", "port": 3306, "user": "root", "passwd": ""}

stream = BinLogStreamReader(
    connection_settings=mysql_settings, server_id=1, auto_position=[]
)

kinesis_client = boto3.client("kinesis", endpoint_url="http://localhost:4568")

stream_name = "test-stream"
partition_key = "id"


def extract_values(row):
    return row["values"]


def transform_record(data):
    return {"Data": json.dumps(data), "PartitionKey": partition_key}


for binlogevent in stream:
    # binlogevent.dump()
    if binlogevent.event_type in [
        BINLOG.WRITE_ROWS_EVENT_V1,
        BINLOG.UPDATE_ROWS_EVENT_V1,
        BINLOG.DELETE_ROWS_EVENT_V1,
        BINLOG.WRITE_ROWS_EVENT_V2,
        BINLOG.UPDATE_ROWS_EVENT_V2,
        BINLOG.DELETE_ROWS_EVENT_V2,
    ]:
        records = list(map(transform_record, map(extract_values, binlogevent.rows)))
        print(records)
        response = kinesis_client.put_records(StreamName=stream_name, Records=records)
        print(response)

stream.close()
