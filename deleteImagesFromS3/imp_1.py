import boto3
from botocore.exceptions import NoCredentialsError



bucket_name = ''
folder_path = ''
aws_access_key = ''
aws_secret_key = ''
aws_region = ''



# Initialize a session using your credentials
session = boto3.Session(
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)


s3 = session.client('s3')

def delete_files_containing_thumbs(bucket_name, folder_path):
    try:
        # List objects in the specified folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

        # Check if the folder has files
        if 'Contents' in response:
            for obj in response['Contents']:
                # Check if 'Thumbs.db' is part of the file name
                if 'Thumbs.db' in obj['Key']:
                    # Delete the file
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    print(f"Deleted: {obj['Key']}")
        else:
            print(f"No files found in {folder_path}")

    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred: {e}")


delete_files_containing_thumbs(bucket_name, folder_path)