import logging
import numpy as np
import pandas as pd
import datetime
from datetime import timezone

from iotfunctions.base import BaseTransformer
from iotfunctions.ui import (UISingle, UIFunctionOutSingle, UISingleItem)
logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ankit-jha/addCustomIotFn@nodata_anomaly_package'

class NoDataAnomalyGenerator(BaseTransformer):
    '''
    This function generates nodata anomaly.
    '''

    def __init__(self, input_item, width, factor, output_item):
        self.input_item = input_item
        self.output_item = output_item
        self.width = int(width)
        self.factor = int(factor)
        super().__init__()

    def execute(self, df):
        currentdt = datetime.datetime.now(timezone.utc)
        logger.debug('Start function execution {}'.format(str(currentdt)))
        timeseries = df.reset_index()
        #Create a zero value series
        additional_values = pd.Series(np.zeros(timeseries[self.input_item].size),index=timeseries.index)
        timestamps_indexes = []
        #Divide the timeseries in (factor)number of splits.Each split will have one anomaly
        for time_splits in np.array_split(timeseries,self.factor):
            if not time_splits.empty:
                start = time_splits.sample().index[0]
                end = min(start+self.width,time_splits.index[-1])
                timestamps_indexes.append((start,end))
        #Create nodata anomalies in every split
        logger.debug('Time stamp indexes {}'.format(timestamps_indexes))
        for start, end in timestamps_indexes:
            additional_values.iloc[start:end] = np.NaN
            timeseries[self.output_item] = additional_values + timeseries[self.input_item]
            logger.debug('Final Dataframe')
            logger.debug(timeseries[self.output_item].iloc[start:end])

        timeseries.set_index(df.index.names,inplace=True)
        logger.debug('End function execution {}'.format(str(currentdt)))
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(
                name='input_item',
                datatype=float,
                description='Item to base anomaly on'
                                              ))

        inputs.append(UISingle(
                name='width',
                datatype=int,
                description='Width of the anomaly created- default 100'
                                              ))

        inputs.append(UISingle(
                name='factor',
                datatype=int,
                description='No. of nodata anomalies to be created'
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With NoData anomalies'
                ))
        return (inputs, outputs)
