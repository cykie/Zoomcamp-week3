import pandas as pd
import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    data['lpep_pickup_datetime_v2'] =data.lpep_pickup_datetime.apply(lambda d: datetime.datetime.fromtimestamp(d.timestamp()).strftime('%Y-%m-%d %H:%M:%S'))
    data['lpep_pickup_date']=data.lpep_pickup_datetime.apply(lambda d: datetime.datetime.fromtimestamp(d.timestamp()).strftime('%Y-%m-%d'))
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
