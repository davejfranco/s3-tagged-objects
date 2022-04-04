import os
import s3mv
import logging
from urllib import parse

#logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Main entrypoint
    """
    
    """
    {
        "invocationSchemaVersion": "1.0",
        "invocationId": "YXNkbGZqYWRmaiBhc2RmdW9hZHNmZGpmaGFzbGtkaGZza2RmaAo",
        "job": {
            "id": "f3cc4f60-61f6-4a2b-8a21-d07600c373ce"
        },
        "tasks": [
            {
                "taskId": "dGFza2lkZ29lc2hlcmUK",
                "s3Key": "customerImage1.jpg",
                "s3VersionId": "1",
                "s3BucketArn": "arn:aws:s3:us-east-1:0123456788:awsexamplebucket1"
            }
        ]
    }

    """
    # Parse job parameters from Amazon S3 batch operations
    invocation_id = event['invocationId']
    invocation_schema_version = event['invocationSchemaVersion']

    results = []
    result_code = None
    result_string = None

    task = event['tasks'][0]
    task_id = task['taskId']

    bucket = task['s3BucketArn'].split(':')[-1]
    obj_key = parse.unquote(task['s3Key'], encoding='utf-8')

    if s3mv.has_tags(bucket, obj_key, s3mv.TAGS):
        try:
            s3mv.move_object(bucket, os.environ['TARGET_BUCKET'], obj_key)
            result_code = 'Succeeded'
            result_string = f"Successfully moved object " \
                                    f"{obj_key} from bucket {bucket}."
            logging.info("{}: {}".format(result_code, result_string))
        except Exception as err:
            result_code = "PermanentFailure"
            result_string = f"Unable to move object " \
                                    f"{obj_key} from bucket {bucket}."
            logging.exception("{}: {}, with exception".format(result_code, result_string, err))
            raise
    else: 
        result_code = 'TemporaryFailure'
        result_string = f"object {obj_key} from bucket {bucket}" \
                                    f"does not contain the searched tag."
        logging.error("{}: {}".format(result_code, result_string))

    results.append({
        'taskId': task_id,
        'resultCode': result_code,
        'resultString': result_string
    })
    return {
        "invocationSchemaVersion": invocation_schema_version,
        "treatMissingKeysAs" : "PermanentFailure",
        "invocationId": invocation_id,
        "results": results
    }
    
