'''Test transform module.

Tests 3 functions:
- clean_transactions
- split_basket
- create_baskets
'''

import pytest
from transform.clean_data import (clean_transactions, create_baskets,
                                  split_basket_item)

def test_clean_transactions():
    # Arrange
    raw_transaction_input = [{
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
    expected_output = [{
        'unique_id': '09-29-2020-ISLE-20-00-54-0',
        'date': '09-29-2020',
        'time': '09:00',
        'location': 'Isle of Wight',
        'first_name': 'Paul',
        'total': 5.0,
        'payment_method': 'CASH',
        'basket': "Regular Luxury hot chocolate - 2.40, Regular Flavoured hot chocolate - Hazelnut - £2.60",
        'date_time': '09-29-2020 09:00'
    }]
    # Act, assert
    assert clean_transactions(raw_transaction_input) == expected_output

@pytest.mark.parametrize('basket_input, expected_output', [
    ('Large Flavoured latte - Hazelnut',
     ['Flavoured latte', 'Hazelnut', 'large']),
    ('Cortado',
     ['Cortado', 'standard', 'standard']),
    ('Regular Flavoured hot chocolate - Caramel',
     ['Flavoured hot chocolate', 'Caramel', 'regular']),
    ('Smoothies - Berry Beautiful',
     ['Smoothies', 'Berry Beautiful', 'standard'])
])
def test_split_basket(basket_input, expected_output):
    '''Test `split_basket_item` splits baskets into [product, flavour, size]'''
    assert split_basket_item(basket_input) == expected_output

def test_create_baskets():
    # Arrange
    example_input = [{
        'unique_id': '09-29-2020-ISLE-0',
        'date': '09-29-2020',
        'time': '09:00',
        'location': 'Isle of Wight',
        'first_name': 'Paul',
        'total': 5.0,
        'payment_method': 'CASH',
        'basket': "Regular Luxury hot chocolate - 2.40, Regular Flavoured hot chocolate - Hazelnut - 2.60"
    }]
    expected_output = [{
        'trans_id': '09-29-2020-ISLE-0',
        'item': 'Luxury hot chocolate',
        'flavour': 'standard',
        'size': 'regular',
        'cost': 2.40
    },
    {
        'trans_id': '09-29-2020-ISLE-0',
        'item': 'Flavoured hot chocolate',
        'flavour': 'Hazelnut',
        'size': 'regular',
        'cost': 2.60
    }]
    # Act, assert
    assert create_baskets(example_input) == expected_output
