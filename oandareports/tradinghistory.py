import os
import shutil
from tqdm import tqdm
from luigi.parameter import Parameter
from luigi import Task, ExternalTask
import pandas as pd
import dask.dataframe as dd
from oandapyV20 import API
import oandapyV20.endpoints.transactions as transactions
from oandareports.helperfiles.task import TargetOutput, Requires, Requirement
from oandareports.helperfiles.target import ParquetTarget

# Todo: Add commenting
# Todo: Add parameter for limiting number of trades for downloading

# pipenv run luigi --module tradinghistory GetTradingHistory --local-scheduler

class S3(ExternalTask):
    output = TargetOutput(os.getenv('S3_location')+'tradinghistory/', target_class=ParquetTarget)

class archieve(Task):
    output = TargetOutput(os.getenv('local_location')+'archive/', target_class=ParquetTarget)

class DownloadS3(ExternalTask):
    requires = Requires()
    other = Requirement(S3)

    if ParquetTarget(os.getenv('local_location') + 'archive/').exists():
        shutil.rmtree(os.getenv('local_location') + 'archive/')

    # Set output location
    output = TargetOutput(os.getenv('local_location')+'archive/', target_class=ParquetTarget)

    def run(self):
        input_target = next(iter(self.input().items()))[1]
        dsk = input_target.read()
        self.output().write(dsk)


class GetTradingHistory(Task):

    storage = Parameter(default='')

    client = API(access_token=os.getenv('TOKEN'))

    output = TargetOutput(os.getenv('local_location') + 'archive/', target_class=ParquetTarget)
    store = TargetOutput(os.getenv('local_location')+ 'trading_history/', target_class=ParquetTarget)
    s3store = TargetOutput(os.getenv('S3_location') + 'tradinghistory/', target_class=ParquetTarget)



    def requires(self):
        if self.storage == 's3':
            if ParquetTarget(os.getenv('S3_location')+'tradinghistory/').exists():
                return [DownloadS3(), archieve()]
        else:
            if ParquetTarget(os.getenv('local_location') + 'archive/').exists():
                return archieve()


    def gettransaction(self, first, last):
        trans = transactions.TransactionIDRange(accountID=os.getenv('ACCOUNT_ID'), params={"from": first, "to": last})
        trans = self.client.request(trans)

        return trans

    def run(self):
        last_trans = int(self.gettransaction(1, 2)['lastTransactionID'])
        pbar = tqdm(last_trans)
        if ParquetTarget(os.getenv('local_location') + 'archive/').exists():
            input_target = next(iter(self.input()))
            dsk = input_target.read()
            #last_trans = 15000
        else:
            trans_df = self.gettransaction(1, 1000)
            df = pd.DataFrame(trans_df['transactions'])
            dsk = dd.from_pandas(df, chunksize=10000)
            #last_trans = 15000

        while int(dsk['id'].astype('int64').max().compute()) < last_trans:
            last_recorded = int(dsk['id'].astype('int64').max().compute())
            trans_df = self.gettransaction(last_recorded, last_recorded + 999)
            df = pd.DataFrame(trans_df['transactions'])
            # TODO: Improve this code
            for i in ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed']:
                try:
                    df = df.drop(columns=i)
                except:
                    pass

            dsk = dd.concat([dsk, df])

            pbar.update(1000)

        # Todo: Rewrite
        for i in ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed']:
            try:
                dsk = dsk.drop(i, axis =1)
            except:
                pass
        self.store().write(dsk)

        if self.storage == 's3':
            self.s3store().write(dsk)
            shutil.rmtree(os.getenv('local_location') + 'archive/')
        print('Finished writing to S3')








