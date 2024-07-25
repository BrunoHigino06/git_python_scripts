import json
import boto3
from datetime import datetime

# This AWS Lambda function interacts with an S3 bucket named 'media-ingest-temporary'.
# It performs the following steps:
# 1. Initializes an S3 client using boto3.
# 2. Retrieves the current date in the format 'YYYY-MM-DD'.
# 3. Iterates through all objects in the specified S3 bucket using pagination.
# 4. Filters objects to include only those with the .mp4 extension and whose LastModified date matches the current date.
# 5. Collects the filtered objects' keys and their last modified timestamps into a list.
# 6. Constructs a JSON report that includes the total count of filtered files and their details.
# 7. Saves the JSON report to the same S3 bucket under the 'report/' directory, naming the file with the current date.
# 8. Returns a success response with the report data if successful.
# 9. Handles any exceptions by returning an error response with a 500 status code and the error message.

def lambda_handler(event, context):
    # Initialize an S3 client
    s3 = boto3.client('s3')
    
    # Specify the bucket name and get the current date
    bucket_name = 'media-ingest-temporary'
    current_date = datetime.now().strftime('%Y-%m-%d')

    try:
        day_objects = []
        all_objects = []
        continuation_token = None

        # Loop to handle paginated results from S3
        while True:
            if continuation_token:
                # List objects with continuation token if it exists
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    ContinuationToken=continuation_token
                )
            else:
                # List objects without continuation token
                response = s3.list_objects_v2(Bucket=bucket_name)

            # Ensure 'Contents' exists in the response
            if 'Contents' in response:
                # Filter objects by .mp4 extension and current date
                filtered_objects = [
                    {'Key': obj['Key'], 'LastModified': obj['LastModified'].isoformat()}
                    for obj in response['Contents']
                    if obj['Key'].endswith('.mp4') and obj['LastModified'].strftime('%Y-%m-%d') == current_date
                ]

                # Get all .mp4 objects
                all_mp4_objects = [
                    {'Key': obj['Key'], 'LastModified': obj['LastModified'].isoformat()}
                    for obj in response['Contents']
                    if obj['Key'].endswith('.mp4')
                ]

                # Append all objects to the list
                all_objects.extend(all_mp4_objects)

                # Append filtered objects to the list
                day_objects.extend(filtered_objects)

            # Check if there are more objects to fetch
            if 'NextContinuationToken' in response:
                continuation_token = response['NextContinuationToken']
            else:
                break

        # Include the total count at the beginning of the JSON data
        output_data = {
            'total_files_until_current_date': len(all_objects),
            'total_files_today': len(day_objects),
            'Files': day_objects
        }

        # Convert the output data to JSON with pretty-printing and save to S3
        json_data = json.dumps(output_data, indent=4)
        file_key = f"report/mp4_objects_{current_date}.json"

        # Upload the JSON report to the specified S3 bucket
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=json_data)

        return {
            'statusCode': 200,
            'body': json.dumps(output_data, indent=4)
        }

    except Exception as e:
        # Handle exceptions by returning a 500 status code and the error message
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}, indent=4)
        }
