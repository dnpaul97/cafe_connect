import csv
import datetime
import logging

import boto3

def get_key_prefix():
     today = datetime.date.today()
     yesterday = today - datetime.timedelta(days=1)
     key_prefix = yesterday.strftime("%Y/%m/%d") 
     return(key_prefix) 

def get_file_names():
    '''Yield all files in bucket that start with today's date.'''
    # List all files in bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket("cafe-transactions-group-1")
    all_files = (file.key for file in bucket.objects.all())
    file_name_start = get_key_prefix()
    for file_name in all_files:
        if file_name.startswith(file_name_start):
            yield file_name

def get_key_suffix(file_name):
    '''Returns last 12 characters of file name.'''
    suffix = file_name[-12:]
    identifier = suffix.replace('.csv', '')
    return identifier.strip()
    
def read_csv_file_from_s3(bucket, key):
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket,
                              Key=key)
    data = s3_object['Body'].read().decode('utf-8')
    return csv.reader(data.splitlines())

def output_raw_transactions(csv_reader, identifier, skip_header=True):
    '''Convert csv reader into a list of dictinaries'''
    raw_transaction_list = []
    if skip_header:
        next(csv_reader)
    counter = 0
    for line in csv_reader:
        try:
            raw_transaction = {
                'date': line[0],
                'location': line[1],
                'customer_name': line[2],
                'basket': line[3],
                'pay_amount': line[4],
                'payment_method': line[5],
                'ccn': line[6],
                'id_number': counter,
                'identity': identifier
            }
            raw_transaction_list.append(raw_transaction)
            counter += 1
        except ValueError:
            log_message = {f'Failed to read csv row': {
                'identifier': identifier,
                'row': line
            }}
            logging.error(log_message)
            continue
    return raw_transaction_list
