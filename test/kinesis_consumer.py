import boto3
from botocore.exceptions  import ClientError
import os
import datetime
import time

kinesis_data_stream_key = "SPORT_DATA_LIVE_COMMENTARIES_STREAM"
kinesis_data_stream = os.getenv(kinesis_data_stream_key, "sports-data-live-commentaries")
shard_id = "shardId-000000000000"


def process_records(Records):
    # ---------------------------
    # Your processing goes here
    # ---------------------------
    print(Records)
    pass


def main():
    stream_name = kinesis_data_stream

    try:
        kinesis_client = boto3.client('kinesis')

        # ------------------
        # Get the shard ID.
        # ------------------
        response = kinesis_client.describe_stream(StreamName=stream_name)
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']

        # ---------------------------------------------------------------------------------------------
        # Get the shard iterator.
        # ShardIteratorType=AT_SEQUENCE_NUMBER|AFTER_SEQUENCE_NUMBER|TRIM_HORIZON|LATEST|AT_TIMESTAMP
        # ---------------------------------------------------------------------------------------------
        response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            Timestamp=datetime.datetime.now().timestamp(),
            ShardIteratorType='AT_TIMESTAMP'
        )
        shard_iterator = response['ShardIterator']

        # -----------------------------------------------------------------
        # Get the records.
        # Get max_records from the shard, or run continuously if you wish.
        # -----------------------------------------------------------------

        while True:
            response = kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=1
            )
            shard_iterator = response['NextShardIterator']
            records = response['Records']
            if len(records) == 0:
                print("No records found.")
            else:
                print(records)
            time.sleep(10)

    except ClientError as e:
        print("Couldn't get records from stream %s.", stream_name)
        raise


if __name__ == "__main__":
    main()