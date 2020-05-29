#!/user/bin/env python3
import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
import numpy as np
import pandas as pd

from iotfunctions.base import BaseTransformer
from iotfunctions import ui

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install

PACKAGE_URL = 'git+https://github.com/ankit-jha/addCustomIotFn@starter_package'

class MultiplyByFactorAJ(BaseTransformer):

    def __init__(self, input_items, entity_list=None, factor, output_items):
        self.input_items = input_items
        self.output_items = output_items
        self.factor = float(factor)
        self.entity_list = entity_list

    def execute(self, df):
        entity_filter = df.index.isin(self.entity_list, level=0) if entity_list is not None else True
        df = df[entity_filter].copy()
        for i,input_item in enumerate(self.input_items):
            df[self.output_items[i]] = df[input_item] * self.factor
        return df 

    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMulti(
                name='entity_list',
                datatype=str,
                description='comma separated list of entity ids',
                required=False)
                )
        inputs.append(ui.UIMultiItem(
                name = 'input_items',
                datatype=float,
                description = "Data items adjust",
                output_item = 'output_items',
                is_output_datatype_derived = True)
                )        
        inputs.append(ui.UISingle(
                name = 'factor',
                datatype=float)
                )
        outputs = []
        return (inputs,outputs)
