'''Provides functions to send SQS messages and split long lists.'''
import json
import logging

import boto3

def send_sqs_message(message, queue_name, queue_url):
    '''Send message text to SQS queue.'''
    sqs_client = boto3.resource('sqs')
    queue = sqs_client.get_queue_by_name(QueueName=queue_name)
    response = queue.send_message(QueueUrl=queue_url, MessageBody=message)
    logging.info({'MessageId': response['MessageId']})

def send_message_list_to_sqs(message_list, queue_name, queue_url, file_name=''):
    '''Sends each message in message_list to SQS queue.'''
    for count, message in enumerate(message_list, start=1):
        try:
            send_sqs_message(message, queue_name=queue_name, queue_url=queue_url)
            logging.info(f'Message {count} sent to SQS')
        except Exception as ERROR:
            log_message = {'Message not sent to SQS': {
                'file_name': file_name,
                'message_number': count,
                'error': str(ERROR),
                'message': message
            }}
            logging.error(log_message)

def split_long_list(listy, max_length=750):
    '''Splits a long list into chunks of `max_length`.'''
    return [listy[i:i+max_length]
            for i in range(0, len(listy), max_length)]
