import logging
import numpy as np
import pandas as pd
import datetime
from datetime import timezone
from sqlalchemy import exists

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

        logger.debug('Dataframe shape {}'.format(df.shape))
        
        entity_type = self.get_entity_type()
        derived_metric_table_name = 'DM_'+self.get_entity_type_param('name')
        schema = entity_type._db_schema

        #Store and initialize the counts by entity id 
        db = self.get_db()
        query, table = db.query(derived_metric_table_name,schema,column_names='KEY',filters={'KEY':self.output_item})
        raw_dataframe = db.get_query_data(query)
        logger.debug('Check for key {} in derived metric table {}'.format(self.output_item,raw_dataframe.shape))
        key = '_'.join([derived_metric_table_name, self.output_item])

        if raw_dataframe is not None and raw_dataframe.empty:
            #Delete old counts if present
            db.cos_delete(key)
            logger.debug('Intialize count for first run')
        
        counts_by_entity_id = db.cos_load(key,binary=True)
        if counts_by_entity_id is None:
            counts_by_entity_id = {}
        logger.debug('Initial Grp Counts {}'.format(counts_by_entity_id))            

        #Mark Anomalies
        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby=timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]
            logger.debug('Group id {}'.format(grp[0]))
            logger.debug('Group Indexes {}'.format(df_entity_grp.index))

            count = 0
            local_std = df_entity_grp.iloc[:10][self.input_item].std()
            if entity_grp_id in counts_by_entity_id:
                count = counts_by_entity_id[entity_grp_id]

            mark_anomaly = False
            for grp_row_index in df_entity_grp.index:
                count += 1
                if count%self.factor == 0:
                    #Mark anomaly point
                    timeseries[self.output_item].iloc[grp_row_index] = np.random.choice([-1, 1]) * self.size * local_std
            
            counts_by_entity_id[entity_grp_id] = count

        logger.debug('Final Grp Counts {}'.format(counts_by_entity_id))

        #Save the group counts to cos
        db.cos_save(counts_by_entity_id,key,binary=True)

        timeseries.set_index(df.index.names,inplace=True)
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
                description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                default=5
                                              ))

        inputs.append(UISingle(
                name='size',
                datatype=int,
                description='Size of extreme anomalies to be created. e.g. 10 will create 10x size extreme anomaly compared to the normal variance',
                default=10
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With Extreme anomalies'
                ))
        return (inputs, outputs)
