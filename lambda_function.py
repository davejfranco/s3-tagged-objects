import os
import boto3


#tags to find
TAGS = {
    "LifeCycle":"Delete",
}

#client
#session = boto3.Session(profile_name=AWS_PROFILE)
s3 = boto3.client('s3') #session.client('s3')


def has_tags(bucket, obj_key, tags):
    """
    Check if object keys has all the specified tags
    """
    try:
        response = s3.get_object_tagging(
            Bucket=bucket,
            Key=obj_key
        )
    except Exception as err:
       print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(obj_key, bucket))
       
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


def move_object(src_bucket, target_bucket, key):
    """
    This function will copy in the given object to the specified target
    S3 bucket and remove it from the source bucket afterwards
    """
    try:
        s3.copy_object(
            Bucket=target_bucket,
            CopySource={
                'Bucket': src_bucket, 
                'Key': key
            },
            Key=key
        )
    except Exception as err:
        print('Error copying object {} from bucket {} to bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, src_bucket, target_bucket))
        raise err
    
    try:
        s3.delete_object(
            Bucket=src_bucket,
            Key=key
        )
    except Exception as err:
        print('Error delete object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, src_bucket))
        raise err
    return

def lambda_handler(event, context):
    """
    Main entrypoint
    """
    
    """
    Event Sample
    {'version': '0', 
    'id': 'b202a966-0de5-0665-2fb8-7673fc47a11c', 
    'detail-type': 'Object Tags Added', 
    'source': 'aws.s3', 
    'account': '784170130350', 
    'time': '2022-02-15T14:07:50Z', 
    'region': 'eu-west-1', 
    'resources': ['arn:aws:s3:::dops-62-doc-shredding'], 
    'detail': {'version': '0', 'bucket': {'name': 'dops-62-doc-shredding'}, 'object': {'key': 'image (6).png', 'etag': 'be947f5ff18f443c4738bc6cc1486c46', 'version-id': '6CWUqeRnhKoOUEBsV2EyTs1LjStR8v0F'}, 
    'request-id': 'HJV2PJDVYHP7A0SQ', 
    'requester': '784170130350', 
    'source-ip-address': '213.32.243.220'}}

    """
    # source bucket and key will produce the copy object required for the copy_object method
    src_bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']

    if has_tags(src_bucket, key, TAGS):
        move_object(src_bucket, os.environ['TARGET_BUCKET'], key)
        print("Object {} moved from bucket: {} to bucket: {}".format(key, src_bucket, os.environ['TARGET_BUCKET']))

    
