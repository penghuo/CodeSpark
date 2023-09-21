import boto3
import json
import random
import time

# Initialize the S3 client
# session = boto3.Session(profile_name='default')
session = boto3.Session()
s3 = session.client('s3')

def generate_and_upload_file(bucket_name, N):
    while N > 0:
        # Get the current timestamp to use in the file name
        current_time = time.strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"{current_time}.{N}.json.bz2"
        # Define the S3 key (path) where the file will be stored
        file_key = f"data/http_log/mock_streaming/{file_name}"

        # Upload the JSON file to S3
        s3.copy_object(
            CopySource={'Bucket': bucket_name, 'Key': "data/http_log/http_logs_partitioned_json_bz2/year=1998/month=4/day=30/part-00000-76cb51e0-1b8f-41ea-8bfd-e77261483002.c000.json.bz2"}, Bucket=bucket_name, 
            Key=file_key)
        print(f"uploaded to S3 bucket '{bucket_name}' at path '{file_key}'")
        N = N - 1

def clean_up_stack():
    # Delete all objects in the specified S3 bucket
    bucket_name = 'flint-data-dp-eu-west-1-beta'
    prefix = 'data/http_log/mock_streaming/'

    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in objects:
        for obj in objects['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    print("Cleaned up S3 bucket.")        

if __name__ == "__main__":
    # file_path = f"/tmp/{random.randint(0, 10)}.json"
    # print(f"write to file_path {file_path}")
    # generate_file(file_path)

    clean_up_stack()
    s3_bucket_name = "flint-data-dp-eu-west-1-beta"

    while True:
        generate_and_upload_file(s3_bucket_name, 10)
        time.sleep(10)  # Sleep for 10 seconds before generating the next file
