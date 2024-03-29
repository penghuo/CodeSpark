import boto3
import json
import random
import time

# Initialize the S3 client
# session = boto3.Session(profile_name='default')
session = boto3.Session()
s3 = session.client('s3')

def generate_file(file_path):
    # Function to generate a random size value between 0 and 1024
    def generate_random_size():
        return random.randint(0, 1024)

    # Generate 1000 JSON records with random size values
    file_content = []
    for _ in range(1000):
        size = generate_random_size()
        record = {
            "@timestamp": "1998-04-30T19:59:06.000Z",
            "clientip": "141.78.0.0",
            "request": "GET /images/space.gif HTTP/1.0",
            "status": 304,
            "size": size
        }
        file_content.append(record)

    # write file
    file_content_json = json.dumps(file_content)
    with open(file_path, 'w') as file:
        file.write(file_content_json)


def generate_and_upload_file(src_file, bucket_name, N):
    while N > 0:
        # Get the current timestamp to use in the file name
        current_time = time.strftime("%Y-%m-%d-%H-%M-%S")
        file_name = f"{current_time}.{N}.json.bz2"
        # Define the S3 key (path) where the file will be stored
        file_key = f"data/http_log/mock_streaming/{file_name}"

        # Upload the JSON file to S3
        s3.upload_file(src_file, bucket_name, file_key)
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
    file_path = "/tmp/part-00000.json.bz2"
    s3_bucket_name = "flint-data-dp-eu-west-1-beta"

    while True:
        generate_and_upload_file(file_path, s3_bucket_name, 10)
        time.sleep(10)  # Sleep for 10 seconds before generating the next file
