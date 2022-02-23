import boto3
from botocore.exceptions import ClientError


#tags to find
TAGS = {
    "LifeCycle":"Delete",
}

#client
s3 = boto3.client('s3')


def has_tags(bucket: str, obj_key: str, tags: dict[str, str]) -> bool:
    """
    Check if object keys has all the specified tags
    """
    try:
        response = s3.get_object_tagging(
            Bucket=bucket,
            Key=obj_key
        )
    except ClientError as err:
        print('Error getting object {} from bucket {}.'.format(obj_key, bucket))
        raise err
    except Exception as err:
       print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(obj_key, bucket))
       return False
       
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


def move_object(src_bucket: str, target_bucket: str, key: str) -> None:
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
        print('Object {} copied from bucket {} but not deleted.'.format(key, src_bucket))
        raise err
    return