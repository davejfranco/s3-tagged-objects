import os
import sys
import boto3
import logging

"""
Event Sample
{   'version': '0', 
    'id': 'bfe9e309-1f50-814d-606d-fbfb82501118', 
    'detail-type': 'Object Tags Added', 
    'source': 'aws.s3', 
    'account': '784170130350', 
    'time': '2022-02-14T15:20:13Z', 
    'region': 'eu-west-1', 
    'resources': ['arn:aws:s3:::sample-s3-bucket'], 
    'detail': {'version': '0', 'bucket': {'name': 'sample-s3-bucket'}, 
    'object': {'key': 'Screen Shot 2022-01-14 at 12.08.04.png', 
    'etag': 'ce450ae73fde2f47ab2918d6f4cdf4a8', 
    'version-id': 'BYFNeaVHpiW1P5WLRm5CJgwEplzBk_PU'}, 
    'request-id': '5GS3JWQA2D8ZZRP5', 
    'requester': '784170130350', 
    'source-ip-address': '213.30.242.220'}}
"""

AWS_PROFILE = "playground"
SRC_BUCKET = "sample-s3-bucket"

#tags to find
TAGS = {
    "LifeCycle":"Delete",
}

# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#client
session = boto3.Session(profile_name=AWS_PROFILE)
s3 = session.client('s3')


def has_tags(bucket: str, obj_key: str, tags: dict[str, str]) -> bool:
    """
    Check if object keys has all the specified tags
    """
    try:
        response = s3.get_object_tagging(
            Bucket=bucket,
            Key=obj_key
        )
    except Exception as err:
       logger.exception('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(obj_key, bucket))
       raise err
    """
    {'ResponseMetadata': 
        {'RequestId': 'GFP8QPPFXQZNAS5V', 
        'HostId': 'TC+miOkl8ix5LXu5hQ0260Wv3omcGQKgvS+DDcE2WcVxZM9EpiA/GUnb89qdVTGZTQG0C+Rw+Ao=', 
        'HTTPStatusCode': 200, 
        'HTTPHeaders': 
            {'x-amz-id-2': 'TC+miOkl8ix5LXu5hQ0260Wv3omcGQKgvS+DDcE2WcVxZM9EpiA/GUnb89qdVTGZTQG0C+Rw+Ao=', 
            'x-amz-request-id': 'GFP8QPPFXQZNAS5V', 
            'date': 'Tue, 15 Feb 2022 09:32:00 GMT', 
            'x-amz-version-id': 'BYFNeaVHpiW1P5WLRm5CJgwEplzBk_PU', 
            'transfer-encoding': 'chunked', 
            'server': 'AmazonS3'}, '
            RetryAttempts': 0}, 
        'VersionId': 'BYFNeaVHpiW1P5WLRm5CJgwEplzBk_PU', 
        'TagSet': [{'Key': 'LifeCycle', 'Value': 'Delete'}, {'Key': 'date', 'Value': 'today'}, {'Key': 'Type', 'Value': 'photo'}]}
    """
    for k, v in tags.items():
        match = 0
        for tag in response['TagSet']:
            if k == tag['Key'] and v == tag['Value']:
                match = 1
        if match == 0:
            return False
    return True            


def move_object(src_bucket: str, target_bucket: str, obj_key: str) -> None:

    try:
        s3.copy_object(
            Bucket=target_bucket,
            CopySource=src_bucket,
            Key=obj_key
        )
    except Exception as err:
        logger.exception('Error copying object {} from bucket {} to bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(obj_key, src_bucket, target_bucket))
        raise err
    
    try:
        s3.delete_object(
            Bucket=src_bucket,
            Key=obj_key
        )
    except Exception as err:
        logger.exception('Error delete object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(obj_key, src_bucket))
        raise err
    return

def lambda_handler(event, context):
    """
    Main entrypoint
    """
    
    return {
        'statusCode': 200
    }

print(has_tags(SRC_BUCKET, "Screen Shot 2022-01-14 at 12.08.04.png", TAGS))
