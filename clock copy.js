import os
import zipfile
import boto3

def lambda_handler(event, context):
    # Source S3 bucket and prefix
    source_bucket_name = 'source-bucket'
    source_prefix = 'path/to/objects/'

    # Destination S3 bucket and key
    destination_bucket_name = 'destination-bucket'
    destination_key = 'path/to/destination.zip'

    # Filter types (e.g., 'txt', 'csv', 'jpg')
    filter_types = ['txt', 'csv']

    # Create a temporary directory to store the files
    temp_dir = '/tmp/files'
    os.makedirs(temp_dir, exist_ok=True)

    # Initialize AWS clients
    s3_client = boto3.client('s3')

    try:
        # List objects in the source bucket
        response = s3_client.list_objects_v2(Bucket=source_bucket_name, Prefix=source_prefix)

        # Get the objects matching the filter types
        objects_to_zip = [obj['Key'] for obj in response['Contents'] if obj['Key'].lower().endswith(tuple(filter_types))]

        # Download the matching objects and store them in the temporary directory
        for obj_key in objects_to_zip:
            file_path = os.path.join(temp_dir, os.path.basename(obj_key))
            s3_client.download_file(source_bucket_name, obj_key, file_path)

        # Zip the files
        zip_file_path = '/tmp/archive.zip'
        with zipfile.ZipFile(zip_file_path, 'w') as zipf:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    zipf.write(file_path, arcname=file)

        # Upload the zip file to the destination bucket
        s3_client.upload_file(zip_file_path, destination_bucket_name, destination_key)

        # Clean up the temporary files
        os.remove(zip_file_path)
        for file in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file)
            os.remove(file_path)
        os.rmdir(temp_dir)

        return {
            'statusCode': 200,
            'body': 'Objects filtered, zipped, and uploaded successfully'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
