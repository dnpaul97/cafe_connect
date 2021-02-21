'''Test extract module.

Tests 2 functions:
- output_raw_transactions
- split_long_list
'''

import csv
from io import StringIO

from extract.read_from_s3 import output_raw_transactions
from extract.sqs_messaging import split_long_list

def test_output_raw_transactions():
    '''Test `output_raw_transactions` returns list of transaction dictionaries.
    '''
    # ARRANGE
    # Create example csv file for function to read
    example_row_string = '29/09/2020 09:00,Isle of Wight,Paul Kifer,"Regular Luxury hot chocolate - 2.40, Regular Flavoured hot chocolate - Hazelnut - £2.60",5.00,CASH,'
    example_row_bytes = StringIO(example_row_string)
    example_csv = csv.reader(example_row_bytes)
    example_identifier = '20-00-54'
    # Create expected class instance output
    expected_output = [{
        'date': '29/09/2020 09:00',
        'location': 'Isle of Wight',
        'customer_name': 'Paul Kifer',
        'basket': "Regular Luxury hot chocolate - 2.40, Regular Flavoured hot chocolate - Hazelnut - £2.60",
        'pay_amount': '5.00',
        'payment_method': 'CASH',
        'ccn': '',
        'id_number': 0,
        'identity': '20-00-54'
    }]
    # ACT
    actual_output = output_raw_transactions(example_csv, identifier=example_identifier, skip_header=False)
    # ASSERT
    assert len(actual_output) == len(expected_output)
    assert actual_output == expected_output

def test_split_long_list():
    # Arrange
    example_input_list = list(range(10))
    max_length = 3
    expected_output = [
        [0, 1, 2],
        [3, 4, 5],
        [6, 7, 8],
        [9]
    ]
    # Act
    actual_output = split_long_list(example_input_list, max_length=max_length)
    # Assert
    assert actual_output == expected_output