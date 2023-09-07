import boto3
import json
import random
import time

def generate_and_upload_file(bucket_name):
    # Get the current timestamp to use in the file name
    current_time = time.strftime("%Y%m%d%H%M%S")
    file_name = f"{current_time}.json"

    # Define the S3 key (path) where the file will be stored
    file_key = f"data/http_log/mock-streaming/mock-http-log/{file_name}"
    

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

    # Convert the list of JSON records to a JSON-formatted string
    file_content_json = json.dumps(file_content)

    # Initialize the S3 client
    # session = boto3.Session(profile_name='default')
    session = boto3.Session()
    s3 = session.client('s3')

    # Upload the JSON file to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=file_content_json,
        ContentType="application/json"
    )

    print(f"File '{file_name}' with 1000 lines uploaded to S3 bucket '{bucket_name}' at path '{file_key}'")

if __name__ == "__main__":
    # Replace 'your-s3-bucket-name' with your actual S3 bucket name
    s3_bucket_name = "flint-data-for-integ-test"

    while True:
        generate_and_upload_file(s3_bucket_name)
        time.sleep(1)  # Sleep for 60 seconds (1 minute) before generating the next file
