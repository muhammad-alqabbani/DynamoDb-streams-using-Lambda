import boto3
import json
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    data = []
    TableName = "mylab_dynamodb_table"
    bucket_name = "abcdynamodbtables"
    
    try:
        print("Initializing AWS resources...")
        s3 = boto3.resource('s3', region_name='us-east-1')
        ddbclient = boto3.client('dynamodb', region_name='us-east-1')
        
        # Check if DynamoDB service is accessible
        try:
            response = ddbclient.list_tables()
            print("DynamoDB service is accessible.")
        except ClientError as e:
            print("DynamoDB service is not accessible. Detailed error: ", e)
            raise e
        
        # Check if the specified DynamoDB table exists
        mytables = response['TableNames']
        if TableName in mytables:
            print(f"DynamoDB table '{TableName}' exists.")
        else:
            print(f"DynamoDB table '{TableName}' does not exist.")
            return {
                'statusCode': 404,
                'body': json.dumps(f"DynamoDB table '{TableName}' not found.")
            }
        
        # Check if S3 bucket exists and is accessible
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
            print(f"S3 bucket '{bucket_name}' exists and is accessible.")
        except ClientError as e:
            print(f"S3 bucket '{bucket_name}' does not exist or is not accessible. Detailed error: ", e)
            raise e
        
        # Scan the DynamoDB table and upload data to S3
        print("Scanning DynamoDB table...")
        allitems = ddbclient.scan(TableName=TableName)
        for item in allitems['Items']:
            item_list = {}
            allKeys = item.keys()
            for k in allKeys:
                value = list(item[k].values())[0]
                item_list[k] = str(value)
            data.append(item_list)
        data = json.dumps(data)
        print("Uploading data to S3...")
        responses3 = s3.Object(bucket_name, 'data.txt').put(Body=data)
        print("Completed Upload to S3")
        
        print("Lambda run completed")
        return {
            'statusCode': 200,
            'body': json.dumps("success")
        }
    except ClientError as e:
        print("Detailed error: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps("error")
        }
    except Exception as e:
        print("Detailed error: ", e)
        return {
            'statusCode': 500,
            'body': json.dumps("error")
        }
