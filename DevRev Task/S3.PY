akey=""
skey=""

import boto3
import os
client=boto3.client('s3',aws_access_key_id=akey,aws_secret_access_key=skey)

bucket_name="project-run-bucket"

for file in os.listdir():
    if ".csv" in file:
        s3_key=str(file)
        try:
            client.head_object(Bucket=bucket_name, Key=s3_key)
            print(f"{s3_key} already exists in S3 bucket.")
        except:
            client.upload_file(file, bucket_name, s3_key)
            print(f"{s3_key} uploaded to S3 bucket.")

# import boto3
# import os

# client=boto3.client('s3',
#                     aws_access_key_id=akey,
#                     aws_secret_access_key=skey)

# for file in os.listdir():
#     if ".csv" in file:
#         upload_file_bucket="project-run-bucket"
#         upload_file_key=str(file)
#         client.upload_file(file,upload_file_bucket,upload_file_key)
#         print(str(file)+"Uploaded to S3 Bucket")

