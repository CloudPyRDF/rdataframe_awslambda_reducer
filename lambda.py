import boto3
import cloudpickle as pickle
import json
import os
import logging
from datetime import datetime
import re
from botocore.exceptions import ClientError


logging.basicConfig(level=logging.DEBUG)
bucket = os.getenv('bucket')

class ReductionTree:
    """ 
    Implementation of operations on indices of nodes of reduction
    binary tree.
    """
    @staticmethod
    def is_left(n):
        return n % 2 == 0

    @staticmethod
    def is_last(n):
        return n == 1

    @staticmethod
    def parent(n):
        return n // 2

    @staticmethod
    def twin(n):
        if ReductionTree.is_left(n):
            return n + 1
        else:
            return n - 1

    @staticmethod
    def leafs_count(file_count):
        return 2 ** (file_count - 1).bit_length()

    @staticmethod
    def exists(n, file_count):
        N = ReductionTree.leafs_count(file_count)
        leaf_number = 2 ** ((N).bit_length() - (n).bit_length()) * n
        return leaf_number < N + file_count


def lambda_handler(event, context):
    print(event)
    event_time = event['Records'][0]['eventTime']
    filename = event['Records'][0]['s3']['object']['key']
    event_time = datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%S.%fZ")

    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    try:
        return run_async(event_time, filename)
    except Exception as e:
        print(e)
        logger.exception(e)

        serialized_exception = json.dumps((type(e).__name__, str(e)))
        return dict(statusCode=500, exception=serialized_exception)

def run_async(event_time, filename):
    prefix, partial_number, file_count = get_args(filename)
    print(f'args: {prefix=}, {partial_number=}, {file_count=}')
    if ReductionTree.is_last(partial_number):
        print(f'it is last')
        s3_copy(src=filename, dst=f'output/{prefix}/final')
        return dict(statusCode=200)

    twin_number = ReductionTree.twin(partial_number)
    parent_number = ReductionTree.parent(partial_number)
    new_filename = get_name(parent_number, prefix, file_count)

    print(f'logging: {twin_number=}, {partial_number=}, {new_filename=}')
    if (ReductionTree.is_left(partial_number) and
            not ReductionTree.exists(twin_number, file_count)):
        print(f'left without twin')
        s3_copy(src=filename, dst=new_filename)
        return dict(statusCode=200)

    twin_name = get_name(twin_number, prefix, file_count)
    twin_event_time = get_last_modified_from_s3(twin_name)

    print(f'logging: {twin_name=}, {twin_event_time=}')
    if twin_event_time is None:
        print(f'could not get twin - probably not yet created')
        return dict(statusCode=404)

#    event_time = event_time.replace(tzinfo=twin_event_time.tzinfo)
#    if twin_event_time == event_time and ReductionTree.is_left(partial_number):
#        print(f'other first')
#        return dict(statusCode=409)
    
    if get_last_modified_from_s3(new_filename) is not None:
        return dict(statusCode=200)


    print('YEAH! Reduction baby!')
    f1 = get_and_deserialize_from_s3(filename)
    f2 = get_and_deserialize_from_s3(twin_name)
    reducer = get_and_deserialize_from_s3(f'output/{prefix}/reducer')
    serialize_and_upload_to_s3(reducer(f1, f2), new_filename)

    return dict(statusCode=200)

def get_args(filename):
    m = re.match(pattern='output/([^/]+)/partial_(\d+)_(\d+).out', string=filename)
    if m is None:
        raise KeyError('Wrong s3 key')

    prefix = m.group(1)
    partial_number = int(m.group(2))
    file_count = int(m.group(3))
    return prefix, partial_number, file_count

def get_last_modified_from_s3(filename):
    client = boto3.client('s3')
    try:
        return client.head_object(Bucket=bucket, Key=filename)
    except (ClientError, KeyError):
        return None

def get_name(number, prefix, file_count):
    return f'output/{prefix}/partial_{number}_{file_count}.out'

def s3_copy(src, dst):
    client = boto3.client('s3')
    client.copy_object(Bucket=bucket, CopySource=f'{bucket}/{src}', Key=dst)

def serialize_and_upload_to_s3(hist, filename):
    pickled_hist = pickle.dumps(hist)
    upload_result_to_s3(pickled_hist, filename)

def get_unique_filename(prefix):
    return f'output/{prefix}/final.pickle'

def upload_result_to_s3(obj: bytes, filename: str):
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=obj, Bucket=bucket, Key=filename)

def get_and_deserialize_from_s3(filename):
    pickled_file = get_file_content_from_s3(filename)
    return pickle.loads(pickled_file)

def get_file_content_from_s3(filename):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=filename)
    return response['Body'].read()

