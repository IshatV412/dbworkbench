import boto3
import os

# Let's use Amazon S3
s3 = boto3.client('s3')

# Print out bucket names
response = s3.list_buckets()

print("Buckets you can view:")
print("-" * 50)

# Path to test image (relative to this script's location)
script_dir = os.path.dirname(os.path.abspath(__file__))
filepath = os.path.join(script_dir, "test.jpeg")

for bucket in response['Buckets']:
    bucket_name = bucket['Name']
    creation_date = bucket['CreationDate']
    print(f"• {bucket_name} (Created: {creation_date})")

    # Upload a new file
    s3.upload_file(filepath, bucket_name, "test")