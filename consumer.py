import sys
import argparse
import boto3
import json
import time

# Initialize the argument parser
parser = argparse.ArgumentParser(description='Access AWS resources')
parser.add_argument('-rb', '--read_bucket', type=str, help='Specify the S3 bucket to read from.')

def retrieve_and_sort(response):
    # Sort the keys
    object_keys = [obj['Key'] for obj in response['Contents']]
    sorted_keys = sorted(object_keys)
    return sorted_keys

def main(args):
    session = boto3.Session()
    s3_client = session.client('s3')

    # List all buckets
    b = s3_client.list_buckets()
    bucket_2 = b['Buckets'][1]  
    bucket_3 = b['Buckets'][2]  

    parsed_args = parser.parse_args(args)
    print(parsed_args)

    # Check if read bucket argument was provided
    if parsed_args.read_bucket:
        print(f'Read S3 Bucket: {parsed_args.read_bucket}')

        # Keep track of the sorted keys
        sorted_keys = []

        while True:
            # List objects in the specified bucket
            response = s3_client.list_objects_v2(Bucket=bucket_2['Name'])
            s3_client.put_object(Bucket=bucket_2['Name'], Key = '22', Body='Data!')
            s3_client.delete_object(Bucket=bucket_2['Name'], Key= '22')

            if 'Contents' in response and len(response['Contents']) > 0:
                # Get and sort keys
                new_sorted_keys = retrieve_and_sort(response)

                # Update sorted_keys only if new keys are available
                if sorted_keys != new_sorted_keys:
                    sorted_keys = new_sorted_keys
                    print(f"Updated Sorted Keys: {len(sorted_keys)}")

                # Process each key in sorted_keys
                for object_key in sorted_keys:
                    # Get the object from S3
                    object_response = s3_client.get_object(Bucket=bucket_2['Name'], Key=object_key)
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
                            widget_json = json.dumps(object_content)

                            #Upload to bucket
                            try:
                                s3_client.put_object(Bucket=bucket_3['Name'], Key=widget_key, Body=widget_json)
                                print(f"Successfully stored Widget in {bucket_3['Name']} with key {widget_key}.")
                            except Exception as e:
                                print(f"Failed to store Widget: {e}")


                        print("Retrieved object content:")
                        print(json.dumps(json_data, indent=4))  # Pretty print the JSON data
                        time.sleep(5)
                    except json.JSONDecodeError:
                        print("Object content is not valid JSON.")
                        print(object_content)  # Print raw content if not JSON

                    # Remove the processed key from sorted_keys
                    sorted_keys.remove(object_key)

            else:
                print("No objects found in the bucket. Waiting for new requests...")

            # Wait for 100 ms before trying again
            time.sleep(0.1)

if __name__ == "__main__":
    main(sys.argv[1:])
