import ROOT
import base64
import boto3
import cloudpickle as pickle
import json
import os
import logging
import time

from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.DEBUG)
bucket = os.getenv('bucket')

class Reducer:

    @staticmethod
    def tree_reduce(reducer, iterable, chunk_size=2, min_size=4):
        """
        Parallel tree reduction.
        At each step objects to reduce are divided into
        groups (chunks) and each chunk is then reduced to one
        object in parallel. Whole process lasts until there is
        at most min_size objects, which are then reduced
        sequentially.
        """

        to_process = iterable

        while len(to_process) > min_size:
            chunks = Reducer.divide_into_chunks(to_process, chunk_size=chunk_size)
            to_process = Reducer.parallel_reduce(chunks, reducer)

        return Reducer.reduce(reducer, to_process)

    @staticmethod
    def divide_into_chunks(iterable, chunk_size=2):
        """
        Divide list into chunks of given size.
        If even division is impossible, leftovers are put in the last entry.

        Returns:
            list: List of tuples each of size chunk_size.

        >>> r = Reducer()
        >>> r.divide_into_chunks([1, 2, 3, 4, 5])
        [(1, 2), (3, 4), (5,)]
        >>> r.divide_into_chunks([1, 2, 3, 4, 5, 6], chunk_size=3)
        [(1, 2, 3), (4, 5, 6)]
        >>> r.divide_into_chunks([], chunk_size=1)
        []

        """

        if chunk_size <= 0:
            return []

        return [tuple(iterable[i:i+chunk_size])
                for i in range(0, len(iterable), chunk_size)]

    @staticmethod
    def parallel_reduce(chunks, reducer):
        with ThreadPoolExecutor(len(chunks)) as executor:
            futures = [executor.submit(Reducer.reduce, reducer, chunk)
                       for chunk in chunks]
            results = [future.result() for future in futures]
        return results
    
    @staticmethod
    def reduce(reducer, iterable):
        if not iterable:
            return None

        acc = iterable[0]
        for i in range(1, len(iterable)):
            acc = reducer(acc, iterable[i])

        return acc


def lambda_handler(event, context):
    logging.info(f'event {event}')

    reducer = pickle.loads(base64.b64decode(event['reducer']))
    filesno = pickle.loads(base64.b64decode(event['filesno']))
    prefix  = pickle.loads(base64.b64decode(event['prefix']))

    result = run(reducer, prefix, filesno)

    return {
        'statusCode': 200,
        # 'body': json.dumps(monitor.get_monitoring_results()),
        'filename': json.dumps(serialize_and_upload_to_s3(result, prefix))
    }

def run(reducer, prefix, filesno):
    file = []
    while filesno > 0:
        files = get_files_from_s3(prefix)
        filesno -= len(files)
        files.append(file)
        file = Reducer.tree_reduce(reducer, files)
        time.sleep(1)

    return file

def get_files_from_s3(prefix):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    files = []
    for obj in my_bucket.objects.filter(Prefix='output/' + prefix):
        files.append(get_partial_result_from_s3(obj.key))
        obj.delete()
    return files

def serialize_and_upload_to_s3(hist, prefix):
    pickled_hist = pickle.dumps(hist)
    filename = get_unique_filename(prefix)
    upload_result_to_s3(pickled_hist, filename)
    return filename

def get_unique_filename(prefix):
    return f'output/{prefix}/final.pickle'
    
def upload_result_to_s3(obj: bytes, filename: str):
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=obj, Bucket=bucket, Key=filename)

def get_partial_result_from_s3(filename):
    pickled_file = get_file_content_from_s3(filename)
    return pickle.loads(pickled_file)

def get_file_content_from_s3(filename):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=filename)
    return response['Body'].read()
