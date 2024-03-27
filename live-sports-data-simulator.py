import json, uuid, boto3
import csv
import os
import time
import argparse

sample_input_csv = "data/simulated_game.csv"
kinesis_data_stream_key = "SPORT_COMMENTARY_KINESIS_DATA_STREAM_SRC"
kinesis_data_stream = os.getenv(kinesis_data_stream_key, "sports-data-live-stream-src")


class KinesisStream(object):

    def __init__(self, stream):
        self.stream = stream

    def _connected_client(self):
        """ Connect to Kinesis Streams """
        return boto3.client('kinesis')

    def send_stream(self, data, partition_key=None):
        """
        data: python dict containing your data.
        partition_key:  set it to some fixed value if you want processing order
                        to be preserved when writing successive records.

                        If your kinesis stream has multiple shards, AWS hashes your
                        partition key to decide which shard to send this record to.

                        Ignore if you don't care for processing order
                        or if this stream only has 1 shard.

                        If your kinesis stream is small, it probably only has 1 shard anyway.
        """

        # If no partition key is given, assume random sharding for even shard write load
        if partition_key == None:
            partition_key = str(uuid.uuid4())

        client = self._connected_client()
        return client.put_record(
            StreamName=self.stream,
            Data=json.dumps(data),
            PartitionKey=partition_key
        )


def iterate_dataset():
    with open(sample_input_csv) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            yield row


dataset_iterator = iterate_dataset()


def read_records():
    return next(dataset_iterator)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--sess_id", help="session ID to publish the data", type=str )
    args = parser.parse_args()

    kinesis = KinesisStream(kinesis_data_stream)
    t_end = time.time() + 60 * 5 #run this loop for 5 minutes
    while time.time() < t_end:
        row = read_records()
        if args.sess_id:
            row['sess_id'] = args.sess_id
        kinesis.send_stream(row)
        time.sleep(15)