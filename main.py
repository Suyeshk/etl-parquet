# Import the necessary libraries
import pandas as pd
import boto3
from io import BytesIO

# Create an S3 client
s3 = boto3.client('s3')

# Set the source and destination bucket names
src_bucket_name = 'bucketsnowflakes47'
dst_bucket_name = 'snowoutput'

# Set the source and destination keys (file names)
src_key = 'csv/data.csv'
dst_key = 'parquet/data2.parquet'

# Use the S3 client to download the object
response = s3.get_object(Bucket=src_bucket_name, Key=src_key)
data = response['Body'].read()

# Convert the downloaded bytes to a DataFrame
df = pd.read_csv(BytesIO(data))

# Add a new column to the DataFrame
df['new_column'] = 'default_value'

# Convert the DataFrame to a Parquet file and upload it to S3
buf = BytesIO()
df.to_parquet(buf, index=False)
buf.seek(0)
s3.upload_fileobj(buf, dst_bucket_name, dst_key)
