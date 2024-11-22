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

def retrieve_and_sort(response):
    # Sort the keys
    object_keys = [obj['Key'] for obj in response['Contents']]
    sorted_keys = sorted(object_keys)
    return sorted_keys

def write_to_database(json_data, session, parsed_args, sorted_keys, object_key, s3_client):
    if 'owner' in json_data:
        json_data['id'] = json_data.pop('owner')
        # Create new attributes from each "name" and "value" in "otherAttributes"
        for attribute in json_data.get("otherAttributes", []):
            name = attribute.get("name")
            value = attribute.get("value")
            if name:  # Ensure "name" key exists and is not empty
                json_data[name] = value  # Add 'name' as a new key with 'value' as its value

        # Remove "otherAttributes" as it's no longer needed
        json_data.pop("otherAttributes", None)

    try:
        # Use put_item to write the item to the specified table
        #API expect data in dictionary format
        # boto3.client(region="us-east-1")
        database = session.resource('dynamodb', region_name="us-east-1")
        table = database.Table(parsed_args.write_database)
    

        logger.info(f"Item written to DynamoDB table {parsed_args.write_database} with key {object_key}")
        sorted_keys.remove(object_key)
        s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)
    except Exception as e:
        logger.error("Failed to write to DynamoDB: %s", e)

def write_to_s3(parsed_args, widget_key, widget_json, s3_client, object_key, sorted_keys):
    try:
        s3_client.put_object(Bucket=parsed_args.write_bucket, Key=widget_key, Body=widget_json)
        logger.info(f"Stored Widget in {parsed_args.write_bucket} with key {widget_key}.")
        sorted_keys.remove(object_key)
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

    # List all buckets
    parsed_args = parser.parse_args(args)
    logger.info("Script started with arguments: %s", parsed_args)

    # Check if read bucket argument was provided
    if parsed_args.read_bucket:
        logger.info(f'Reading S3 Bucket: {parsed_args.read_bucket}')

        # Keep track of the sorted keys
        sorted_keys = []

        while True:
            # List objects in the specified bucket
            '''response = s3_client.list_objects_v2(Bucket=bucket_2['Name'])'''
            response = s3_client.list_objects_v2(Bucket = parsed_args.read_bucket)


            if 'Contents' in response and len(response['Contents']) > 0:
                # Get and sort keys
                new_sorted_keys = retrieve_and_sort(response)

                # Update sorted_keys only if new keys are available
                if sorted_keys != new_sorted_keys:
                    sorted_keys = new_sorted_keys
                    logger.info(f"Updated Sorted Keys: {len(sorted_keys)}")

                # Process each key in sorted_keys
                for object_key in sorted_keys:
                    # Get the object from S3
                    object_response = s3_client.get_object(Bucket=parsed_args.read_bucket, Key=object_key)
                    object_content = object_response['Body'].read().decode('utf-8')
                    # Process the object content
                    try:
                        #process the json into a python dictionary
                        json_data = json.loads(object_content)
                      
                        if json_data["type"] == "create":
                            owner = json_data["owner"]
                            #replace spaces with dashes and make it all lowercase
                            owner = owner.replace(" ", "-").lower()

                            widget_id = json_data["widgetId"]
                            widget_key = f"widgets/{owner}/{widget_id}"
                            #Serialize to json string
                            widget_json = json.dumps(json_data)

                            #Upload to bucket
                            if parsed_args.write_bucket != None:
                                write_to_s3(parsed_args, widget_key, widget_json, s3_client, object_key, sorted_keys)

                            if parsed_args.write_database != None:
                                write_to_database(json_data, session, parsed_args, sorted_keys, object_key, s3_client)
                        
                        elif json_data["type"] == "update":
                            # Construct the widget key using owner and widgetId
                            owner = json_data.get("owner", "").replace(" ", "-").lower()
                            widget_id = json_data.get("widgetId")
                            
                            if not owner or not widget_id:
                                logger.warning("Update request missing required fields: 'owner' or 'widgetId'.")
                                continue  # Skip this request
                                # Remove the prefix if present
                            if widget_id.startswith("widgets/"):
                                widget_id = widget_id.split("/", 2)[-1]  # Keeps only the part after the second '/'
                            widget_key = f"widgets/{owner}/{widget_id}"


                            try:
                                # Fetch the current widget data from S3
                                response = s3_client.get_object(Bucket=parsed_args.write_bucket, Key=widget_key)
                                existing_data = json.loads(response['Body'].read().decode('utf-8'))

                                # Update fields in the existing data
                                if "description" in json_data:
                                    existing_data["description"] = json_data["description"]

                                # Add or update otherAttributes
                                for attribute in json_data.get("otherAttributes", []):
                                    name = attribute.get("name")
                                    value = attribute.get("value")
                                    if name:  # Ensure valid attribute name
                                        existing_data[name] = value

                                # Save the updated data back to S3
                                updated_json = json.dumps(existing_data)

                                s3_client.put_object(Bucket=parsed_args.write_bucket, Key=widget_key, Body=updated_json)
                                logger.info(f"Updated widget with key: {widget_key}")
                                s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)

                            except s3_client.exceptions.NoSuchKey:
                                logger.warning(f"Widget with key {widget_key} not found. Cannot update.")
                                s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)
                            except Exception as e:
                                logger.error(f"Failed to update widget: {e}")


                        elif json_data["type"] == "delete":
                            owner = json_data.get("owner", "").replace(" ", "-").lower()
                            widget_id = json_data.get("widgetId")

                            if not owner or not widget_id:
                                logger.warning("Update request missing required fields: 'owner' or 'widgetId'.")
                            widget_key = f"widgets/{owner}/{widget_id}"
                            if not widget_key:
                                logger.warning("Delete request missing widgetKey.")
                                return
                            try:
                                s3_client.delete_object(Bucket=parsed_args.write_bucket, Key=widget_key)
                                logger.info(f"Deleted widget with key: {widget_key}")
                                s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)
                            except Exception as e:
                                logger.error("Failed to delete widget : %s", e)
                                s3_client.delete_object(Bucket=parsed_args.read_bucket, Key=object_key)

                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON content in object: %s", object_key)
                        logger.debug("Object content: %s", object_content)

                    # Remove the processed key from sorted_keys
            else:
                logger.info("No objects found in bucket. Waiting for new requests...")

            # Wait for 100 ms before trying again
            time.sleep(1)
            
    #Similar process for a queue
    elif parsed_args.read_queue:
        pass
        

if __name__ == "__main__":
    main(sys.argv[1:])
