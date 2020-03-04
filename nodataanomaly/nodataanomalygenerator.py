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
            db.model_store.delete_model(key)
            logger.debug('Intialize count for first run')

        counts_by_entity_id = db.model_store.retrieve_model(key,deserialize=False)
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
            logger.debug('Group {} Indexes {}'.format(grp[0],df_entity_grp.index))

            count = 0
            width = self.width
            if entity_grp_id in counts_by_entity_id:
                count = counts_by_entity_id[entity_grp_id][0]
                width = counts_by_entity_id[entity_grp_id][1]

            mark_anomaly = False
            for grp_row_index in df_entity_grp.index:
                count += 1
                
                if width!=self.width or count%self.factor == 0:
                    #Start marking points
                    mark_anomaly = True

                if mark_anomaly:
                    timeseries[self.output_item].iloc[grp_row_index] = np.NaN
                    width -= 1
                    logger.debug('Anomaly Index Value{}'.format(grp_row_index))

                if width==0:
                    #End marking points
                    mark_anomaly = False
                    #Update values
                    width = self.width
                    count = 0

            counts_by_entity_id[entity_grp_id] = (count,width)

        logger.debug('Final Grp Counts {}'.format(counts_by_entity_id))

        #Save the group counts to db
        db.model_store.store_model(key, counts_by_entity_id)

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
                default=10
                                              ))

        inputs.append(UISingle(
                name='width',
                datatype=int,
                description='Width of the anomaly created',
                default=5
                                              ))

        outputs = []
        outputs.append(UIFunctionOutSingle(
                name='output_item',
                datatype=float,
                description='Generated Item With NoData anomalies'
                ))
        return (inputs, outputs)
