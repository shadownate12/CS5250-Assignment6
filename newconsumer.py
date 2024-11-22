import sys
import argparse
import boto3
import json
import time
import logging

# Initialize the argument parser
parser = argparse.ArgumentParser(description='Access AWS resources')
parser.add_argument('-rb', '--read_bucket', type=str, help='Specify the S3 bucket to read from.')
parser.add_argument('-wb', '--write_bucket', type=str, help='Specify the S3 bucket to write to.')
parser.add_argument('-dwt', '--write_database', type=str, help='Specify the database to write to.')
parser.add_argument('-rq', '--read_queue', type=str, help='Specify the SQS Queue to read from.')

def get_oldest_object(response):
    """Retrieve the oldest object based on LastModified."""
    if 'Contents' in response:
        return min(response['Contents'], key=lambda obj: obj['LastModified'])
    return None

def write_to_database(json_data, session, parsed_args, object_key, s3_client):
    if 'owner' in json_data:
        json_data['id'] = json_data.pop('owner')
        for attribute in json_data.get("otherAttributes", []):
            name = attribute.get("name")
            value = attribute.get("value")
            if name:
                json_data[name] = value
        json_data.pop("otherAttributes", None)

    try:
        database = session.resource('dynamodb', region_name="us-east-1")
        table = database.Table(parsed_args.write_database)
        table.put_item(Item=json_data)
        logger.info(f"Item written to DynamoDB table {parsed_args.write_database} with key {object_key}")
        s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)
    except Exception as e:
        logger.error("Failed to write to DynamoDB: %s", e)

def write_to_s3(parsed_args, widget_key, widget_json, s3_client, object_key):
    try:
        s3_client.put_object(Bucket=parsed_args.write_bucket, Key=widget_key, Body=widget_json)
        logger.info(f"Stored Widget in {parsed_args.write_bucket} with key {widget_key}.")
        s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)
    except Exception as e:
        logger.error("Failed to store Widget: %s", e)

# Set up logging configuration
logging.basicConfig(
    filename='consumer.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger()

def main(args):
    session = boto3.Session(region_name="us-east-1")
    s3_client = session.client('s3')

    parsed_args = parser.parse_args(args)
    logger.info("Script started with arguments: %s", parsed_args)

    if parsed_args.read_bucket:
        logger.info(f'Reading S3 Bucket: {parsed_args.read_bucket}')

        while True:
            response = s3_client.list_objects_v2(Bucket=parsed_args.read_bucket)
            oldest_object = get_oldest_object(response)

            if oldest_object:
                object_key = oldest_object['Key']
                logger.info(f"Processing oldest object: {object_key}")
                object_response = s3_client.get_object(Bucket=parsed_args.read_bucket, Key=object_key)
                object_content = object_response['Body'].read().decode('utf-8')

                try:
                    json_data = json.loads(object_content)

                    if json_data["type"] == "create":
                        owner = json_data["owner"]
                        owner = owner.replace(" ", "-").lower()
                        widget_id = json_data["widgetId"]
                        widget_key = f"widgets/{owner}/{widget_id}"
                        widget_json = json.dumps(json_data)

                        if parsed_args.write_bucket:
                            write_to_s3(parsed_args, widget_key, widget_json, s3_client, object_key)

                        if parsed_args.write_database:
                            write_to_database(json_data, session, parsed_args, object_key, s3_client)

                except json.JSONDecodeError:
                    logger.warning("Invalid JSON content in object: %s", object_key)
                    logger.debug("Object content: %s", object_content)
            else:
                logger.info("No objects found in bucket. Waiting for new requests...")

            time.sleep(1)

    elif parsed_args.read_queue:
        pass

if __name__ == "__main__":
    main(sys.argv[1:])
