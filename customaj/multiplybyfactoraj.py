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

    def __init__(self, input_items, factor, output_items, expression, entity_list=None):
        self.input_items = input_items
        self.factor = float(factor)
        self.output_items = output_items
        self.expression = expression
        self.entity_list = entity_list

    def execute(self, df):
        #if self.entity_list:
        #    entity_filter = df.index.isin(self.entity_list, level=0)
        #else:
        #    entity_filter = np.full(len(df),True)

        logger.info('ExpressionWithFilter  exp: ' + self.expression + '  input: ' + str(df.columns))
        expr = self.expression

        if '${' in expr:
            expr = re.sub(r"\$\{(\w+)\}", r"df['\1']", expr)
            msg = 'Expression converted to %s. ' % expr
        else:
            msg = 'Expression (%s). ' % expr

        self.trace_append(msg)

        expr = str(expr)
        logger.info('ExpressionWithFilter  - after regexp: ' + expr)

        try:
            mask = eval(expr)
            for i,input_item in enumerate(self.input_items):
                df[self.output_items[i]] = df[input_item].where(mask) * self.factor
            return df
        except Exception as e:
            logger.info('ExpressionWithFilter  eval for ' + expr + ' failed with ' + str(e))


    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        #inputs.append(ui.UISingleItem(name='dimension_name', datatype=str))
        inputs.append(ui.UIMulti(
                name='entity_list',
                datatype=str,
                description='comma separated list of entity ids',
                required=False)
                )
        inputs.append(ui.UIExpression(
                name='expression',
                description="Define alert expression using pandas systax. \
                             Example: df['inlet_temperature']>50. ${pressure} \
                             will be substituted with df['pressure'] before \
                             evaluation, ${} with df[<dimension_name>]")
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
