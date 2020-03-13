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
        derived_metric_table_name = 'DM_' + entity_type.logical_name
        schema = entity_type._db_schema

        # store and initialize the counts by entity id
        db = self._entity_type.db

        raw_dataframe = None
        try:
            query, table = db.query(derived_metric_table_name, schema, column_names='KEY', filters={'KEY': self.output_item})
            raw_dataframe = db.get_query_data(query)
            key = '_'.join([derived_metric_table_name, self.output_item])
            logger.debug('Check for key {} in derived metric table {}'.format(self.output_item, raw_dataframe.shape))
        except Exception as e:
            logger.error('Checking for derived metric table %s failed with %s.' % (str(self.output_item), str(e)))
            key = str(derived_metric_table_name) + str(self.output_item)
            pass

        if raw_dataframe is not None and raw_dataframe.empty:
            # delete old counts if present
            db.model_store.delete_model(key)
            logger.debug('Reintialize count')

        counts_by_entity_id = None
        try:
            counts_by_entity_id = db.model_store.retrieve_model(key)
        except Exception as e2:
            logger.error('Counts by entity id not yet initialized - error: ' + str(e2))
            pass

        if counts_by_entity_id is None:
            counts_by_entity_id = {}
        logger.debug('Initial Grp Counts {}'.format(counts_by_entity_id))

        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby = timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]

            #Initialize group counts
            count = 0
            if entity_grp_id in counts_by_entity_id:
                count = counts_by_entity_id[entity_grp_id]

            #Start index based on counts and factor
            if count == 0 or count%self.factor == 0:
                strt_idx = 0
            else:
                strt_idx = self.factor - count%self.factor

            #Update group counts for storage
            actual = df_entity_grp[self.output_item].values
            count += actual.size
            counts_by_entity_id[entity_grp_id] = count

            #Prepare numpy array for marking anomalies
            a = actual[strt_idx:]
            # Create NaN padding for reshaping
            nan_arr = np.repeat(np.nan, self.factor - a.size % self.factor)
            # Prepare numpy array to reshape
            a_reshape_arr = np.append(a,nan_arr)
            # Final numpy array to be transformed
            a1 = np.reshape(a_reshape_arr, (-1, self.factor)).T
            # Calculate 'local' standard deviation if it exceeds 1 to generate anomalies
            std = np.std(a1, axis=0)
            stdvec = np.maximum(np.where(np.isnan(std),1,std), np.ones(a1[0].size))
            # Mark Extreme anomalies
            a1[0] = np.multiply(a1[0],
                        np.multiply(np.random.choice([-1,1], a1.shape[1]),
                                    stdvec * self.size))
            # Flattening back to 1D array
            a2 = a1.T.flatten()
            # Removing NaN padding
            a2 = a2[~np.isnan(a2)]
            # Adding the missing elements to create final array
            final = np.append(actual[:strt_idx],a2)
            # Set values in the original dataframe
            timeseries.loc[df_entity_grp.index, self.output_item] = final
        logger.debug(timeseries[['id',self.input_item,self.output_item]].sort_values(['id']))

        logger.debug('Final Grp Counts {}'.format(counts_by_entity_id))

        # save the group counts to db
        try:
            db.model_store.store_model(key, counts_by_entity_id)
        except Exception as e3:
            logger.error('Counts by entity id cannot be stored - error: ' + str(e3))
            pass

        timeseries.set_index(df.index.names, inplace=True)
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
