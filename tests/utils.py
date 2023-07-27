import boto3

s3 = boto3.client('s3')

VERSIONING_BUCKET = 'versioning-article'
VERSIONING_REPO = 'versioning-article'


def s3_file_count(prefix: str = ''):
    response = s3.list_objects_v2(Bucket='versioning-article', Prefix=prefix)
    return response['KeyCount']
