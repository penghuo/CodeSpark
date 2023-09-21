import boto3
import json
import random
import time

# Initialize the S3 client
# session = boto3.Session(profile_name='default')
session = boto3.Session()
s3 = session.client('s3')

def clean_up_stack():
    # Delete all objects in the specified S3 bucket
    bucket_name = 'flint-data-dp-eu-west-1-beta'
    prefix = 'data/http_log/mock_streaming/'

    # Initialize a marker for pagination
    marker = None

    # Loop through paginated results
    while True:
        # List objects with the specified prefix and marker
        if marker == None:
          objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        else:
          objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=marker)
        # If there are objects with the specified prefix, delete them
        if 'Contents' in objects:
            objects_to_delete = [{'Key': obj['Key']} for obj in objects['Contents']]
            s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        else:
            print(f"No objects found with prefix '{prefix}'.")

        # Check if there are more objects to paginate through
        if 'NextContinuationToken' in objects:
            marker = objects['NextContinuationToken']
        else:
            break    
    print("Cleaned up S3 bucket.")
      

if __name__ == "__main__":
    # file_path = f"/tmp/{random.randint(0, 10)}.json"
    # print(f"write to file_path {file_path}")
    # generate_file(file_path)

    clean_up_stack()
    s3_bucket_name = "flint-data-dp-eu-west-1-beta"
