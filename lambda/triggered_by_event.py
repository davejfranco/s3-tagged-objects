import os
import s3mv
import logging

#logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
    event_id = event['id']
    src_bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']

    if s3mv.has_tags(src_bucket, key, s3mv.TAGS):
        s3mv.move_object(src_bucket, os.environ['TARGET_BUCKET'], key)
        #log moved object
        logger.info('Successfully moved object: {} from bucket: {} to bucket: {}'.format(key, src_bucket, os.environ['TARGET_BUCKET']))
    return
    
