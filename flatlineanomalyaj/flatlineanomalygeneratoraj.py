import logging
import numpy as np
import pandas as pd
import datetime

from iotfunctions.base import BaseTransformer
from iotfunctions.ui import (UISingle, UIFunctionOutSingle, UISingleItem)
logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ankit-jha/addCustomIotFn@flatline_anomaly_package'

class FlatlineAnomalyGenerator(BaseTransformer):
    '''
    This function generates flatline anomaly.
    '''

    def __init__(self, input_item, width, factor, output_item):
        self.input_item = input_item
        self.output_item = output_item
        self.width = int(width)
        self.factor = int(factor)
        super().__init__()

    def execute(self, df):
        currentdt = datetime.datetime.now()
        logger.debug("-----------....")
        logger.debug(str(currentdt))
        timeseries = df.reset_index()
        #Create a zero value series
        additional_values = pd.Series(np.zeros(timeseries[self.input_item].size),index=timeseries.index)
        timestamps_indexes = []
        #Divide the timeseries in (factor)number of splits.Each split will have one anomaly
        logger.debug('Time Splits')
        logger.debug(np.array_split(timeseries,self.factor))
        for time_splits in np.array_split(timeseries,self.factor):
            if not time_splits.empty:
                start = time_splits.sample(1).index[0]
                end = min(start+self.width,time_splits.index[-1])
                timestamps_indexes.append((start,end))
        #Create flatline anomalies in every split
        for start, end in timestamps_indexes:
            local_mean = timeseries.iloc[max(0, start - 10):end + 10][self.input_item].mean()
            logger.debug("local mean {}".format(local_mean))
            additional_values.iloc[start:end] += local_mean - timeseries[self.input_item].iloc[start:end]
            logger.debug("additional_values {}".format(additional_values.iloc[start:end]))
            logger.debug("time_series input item values {}".format(timeseries[self.input_item].iloc[start:end]))
            timeseries[self.output_item] = additional_values + timeseries[self.input_item]

        timeseries.set_index(df.index.names,inplace=True)
        logger.debug("-----------....")
        logger.debug(str(currentdt))
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
                description='No. of flatline anomalies to be created'
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With Flatline anomalies'
                ))
        return (inputs, outputs)
