
# Todo: Add comments
# Todo: Add parameter to delete local copy
# Todo: Automatic deletion of S3 after download

import os
from luigi.parameter import Parameter
from luigi import Task, ExternalTask

import oandapyV20.endpoints.instruments as v20instruments
from oandapyV20 import API
import pandas as pd
import dask.dataframe as dd
from dotenv import load_dotenv
load_dotenv()

from helperfiles.task import TargetOutput, Requires, Requirement
from helperfiles.target import ParquetTarget

class env_workaround():
    def return_env(self, value):
        value_tmp = os.getenv(value)
        if value_tmp == None:
            if value == 'OandaEnv':
                value_tmp == 'practice'
            else:
                value_tmp = 'not_availiable'
        return value_tmp

class S3(ExternalTask):
    output = TargetOutput(env_workaround().return_env('S3_location')+'historicdata/', target_class=ParquetTarget)

class DownloadS3(ExternalTask):

    requires = Requires()
    other = Requirement(S3)

    # Set output location
    output = TargetOutput(env_workaround().return_env('local_location')+'rates/', target_class=ParquetTarget)

    def run(self):
        input_target = next(iter(self.input().items()))[1]
        dsk = input_target.read()
        self.output().write(dsk)

class GetHistoricRates(Task):

    storage = Parameter(default='')
    instrument = Parameter()
    granularity = Parameter()

    client = API(access_token=env_workaround().return_env('TOKEN'), environment='practice')

    def output(self):
        return ParquetTarget(env_workaround().return_env(self, 'local_location') + 'rates/' + self.instrument + '_' + self.granularity + '/')

    def s3output(self):
        return ParquetTarget(env_workaround().return_env(self, 'S3_location') + 'rates/' + self.instrument + '_' + self.granularity + '/')

    s3store = TargetOutput(env_workaround().return_env('S3_location') + 'historicdata/', target_class=ParquetTarget)

    requires = Requires()

    def fetch(self):
        params = {"count": 5000, "granularity": self.granularity}
        r = v20instruments.InstrumentsCandles(instrument=self.instrument, params=params)
        self.client.request(r)
        g = r.response['candles']
        tempdf = pd.DataFrame(g)
        return pd.concat([tempdf.drop('mid', axis=1), tempdf['mid'].apply(pd.Series)], axis=1)

    def run(self):
        dsk = None
        if ParquetTarget(env_workaround().return_env(self, 'local_location') + 'rates/' + self.instrument +'/').exists():
            input_target = next(iter(self.input()))
            dsk = input_target.read()

        df = self.fetch()

        if dsk != None:
            dsk2 = dd.from_pandas(df, chunksize=10000)
            dsk = dd.concat([dsk, dsk2])
            dsk = dsk.drop_duplicates()

        else:
            dsk = dd.from_pandas(df, chunksize=10000)

        self.output().write(dsk)

        if self.storage == 's3':
            self.s3output().write(dsk)
