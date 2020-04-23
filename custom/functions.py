import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseAggregator,BaseSimpleAggregator
from iotfunctions import ui
from iotfunctions.ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti,UIExpression)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/ankit-jha/addCustomIotFn@starter_agg_package'

def _general_aggregator_input():
    return {
        'name': 'source',
        'description': 'Select the data item that you want to use as input for your calculation.',
        'type': 'DATA_ITEM',
        'required': True,
    }.copy()

def _number_aggregator_input():
    input_item = _general_aggregator_input()
    input_item['dataType'] = 'NUMBER'
    return input_item

def _no_datatype_aggregator_output():
    return {
        'name': 'name',
        'description': 'Enter a name for the data item that is produced as a result of this calculation.'
    }.copy()

def _general_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataTypeFrom'] = 'source'
    return output_item

def _number_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataType'] = 'NUMBER'
    return output_item

def _no_datatype_aggregator_output():
    return {'name': 'name',
            'description': 'Enter a name for the data item that is produced as a result of this calculation.'}.copy()

def _generate_metadata(cls, metadata):
    common_metadata = {
        'name': cls.__name__,
        'moduleAndTargetName': '%s.%s' % (cls.__module__, cls.__name__),
        'category': 'AGGREGATOR',
        'input': [
            _general_aggregator_input()
        ],
        'output': [
            _general_aggregator_output()
        ]
    }
    common_metadata.update(metadata)
    return common_metadata

class HelloWorldAggregator(BaseAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {
            'description': 'Create simple aggregation using expression on a data item.', 
            'input': [
                _general_aggregator_input(),
                {
                    'name': 'expression',
                    'description': 'Use ${GROUP} to reference the current grain. All Pandas Series methods can be used on the grain. For example, ${GROUP}.max() - ${GROUP}.min().',
                    'type': 'CONSTANT',
                    'required': True,
                    'dataType': 'LITERAL'
                }
            ],
            'output': [
                _no_datatype_aggregator_output()
            ]
        })


    def __init__(self, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.expression = expression

    def execute(self, group):
        return eval(re.sub(r"\$\{GROUP\}", r"group", self.expression))

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UIMultiItem(name='source', datatype=None, description=('Choose the data items'
                                                                            ' that you would like to'
                                                                                  ' aggregate'),
                                  output_item='name', is_output_datatype_derived=True))
        inputs.append(UIExpression(name='expression', description='Paste in or type an AS expression'))
        return (inputs, [])
