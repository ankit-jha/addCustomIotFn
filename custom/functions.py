import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseSimpleAggregator
from iotfunctions import ui
from iotfunctions.ui import (UISingle, UIMultiItem, UIFunctionOutSingle, UISingleItem, UIFunctionOutMulti,UIExpression)

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/starter_agg_package@'


#def _no_datatype_aggregator_output():
#    return {'name': 'name',
#            'description': 'Enter a name for the data item that is produced as a result of this calculation.'}.copy()
#
#
#def _general_aggregator_input():
#    return {'name': 'source', 'description': 'Select the data item that you want to use as input for your calculation.',
#            'type': 'DATA_ITEM', 'required': True, }.copy()
#
#
#def _general_aggregator_output():
#    output_item = _no_datatype_aggregator_output()
#    output_item['dataTypeFrom'] = 'source'
#    return output_item
#
#
#def _number_aggregator_output():
#    output_item = _no_datatype_aggregator_output()
#    output_item['dataType'] = 'NUMBER'
#    return output_item
#
#
#def _generate_metadata(cls, metadata):
#    common_metadata = {'name': cls.__name__, 'moduleAndTargetName': '%s.%s' % (cls.__module__, cls.__name__),
#                       'category': 'AGGREGATOR', 'input': [_general_aggregator_input()], 'output': [_general_aggregator_output()]}
#    common_metadata.update(metadata)
#    return common_metadata

class HelloWorldAggregator(BaseSimpleAggregator):
    '''
    Create aggregation using expression on a data item.
    '''

    def __init__(self, source=None, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.source = source
        self.expression = expression
        super().__init__()

    def execute(self, group):
        return eval(re.sub(r"\$\{GROUP\}", r"group", self.expression))

    #def aggregate(self, x):
    #    pass

    @classmethod
    def build_ui(cls):
        inputs = []
        #inputs.append(UISingleItem(
        #        name='source',
        #        datatype=None,
        #        description='Choose the data items that you would like to aggregate on'
        #        ))
        inputs.append(UIMultiItem(
                name='source',
                datatype=None,
                description='Choose the data items \
                        that you would like to aggregate on'),
                output_item='name',
                is_output_datatype_derived=True
                )

        inputs.append(UIExpression(
                name='expression',
                description='Paste in or type an AS expression'
                ))

        outputs = []
        #outputs.append(UIFunctionOutSingle(
        #        name='name',
        #        is_output_datatype_derived=True,
        #        description='Generated Aggregation'
        #        ))
        return (inputs, outputs)
