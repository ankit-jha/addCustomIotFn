import logging
import numpy as np
import pandas as pd
import datetime
from datetime import timezone

from iotfunctions.base import BaseTransformer
from iotfunctions.ui import (UISingle, UIFunctionOutSingle, UISingleItem)
logger = logging.getLogger(__name__)

PACKAGE_URL = 'git+https://github.com/ankit-jha/addCustomIotFn@extreme_anomaly_package'

class ExtremeAnomalyGenerator(BaseTransformer):
    '''
    This function generates extreme anomaly.
    '''

    def __init__(self, input_item, factor, size, output_item):
        self.input_item = input_item
        self.output_item = output_item
        self.factor = int(factor)
        self.size = int(size)
        super().__init__()

    def execute(self, df):
        currentdt = datetime.datetime.now(timezone.utc)
        logger.debug('Start function execution {}'.format(str(currentdt)))
        timeseries = df.reset_index()
        #Create a zero value series
        additional_values = pd.Series(np.zeros(timeseries[self.input_item].size),index=timeseries.index)
        timestamps_indexes = []
        logger.debug('Dataframe shape {}'.format(df.shape))

        # #Get Derived Metric Table
        derived_metric_table_name = 'DM_'+self.get_entity_type_param('name')
        derived_metric_table_key = self.input_item
        df_deviceid_col_name = 'entity_id'
        db = self.get_db()
        schema = "BLUADMIN"
        logger.debug('Fire Database query')
        print('Fire Database query')
        df_result = db.read_agg(derived_metric_table_name,schema,{df_deviceid_col_name: 'count'},group_by=df_deviceid_col_name)
        logger.debug(df_result)

        logger.debug('Entity table name {}'.format(self.get_entity_type_param('name')))
        print('Entity table name {}'.format(self.get_entity_type_param('name')))
        print('Entity type {}'.format(self.get_entity_type()))
        
        #logger.debug('Entity metricsTableName {}'.format(self.get_entity_type().get_attributes_dict()))
        logger.debug('Entity db {}'.format(self.get_db()))
        logger.debug('Entity metadata {}'.format(self.get_db().entity_type_metadata.items()))
        # entity_type_metadata = self.get_db().entity_type_metadata[self.get_entity_type_param('name')]
        # logger.debug('Entity metadata type {}'.format(type(entity_type_metadata)))
        # logger.debug('Entity metadata data items {}'.format(entity_type_metadata._data_items))


        #Divide the timeseries in (factor)number of splits.Each split will have one anomaly
        for time_splits in np.array_split(timeseries,self.factor):
            if not time_splits.empty:
                start = time_splits.sample().index[0]
                timestamps_indexes.append(start)
        #Create extreme anomalies in every split
        logger.debug('Time stamp indexes {}'.format(timestamps_indexes))
        for start  in timestamps_indexes:
            local_std = timeseries[self.input_item].iloc[max(0, start - 10):start + 10].std()
            additional_values.iloc[start] += np.random.choice([-1, 1]) * self.size * local_std
            timeseries[self.output_item] = additional_values + timeseries[self.input_item]

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
                name='factor',
                datatype=int,
                description='No. of extreme anomalies to be created'
                                              ))

        inputs.append(UISingle(
                name='size',
                datatype=int,
                description='Size of extreme anomalies to be created. e.g. 10 will create 10x size extreme anomaly compared to the normal variance'
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With Extreme anomalies'
                ))
        return (inputs, outputs)
