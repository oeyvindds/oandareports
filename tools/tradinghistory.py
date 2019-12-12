import os
import shutil
from tqdm import tqdm
from luigi.parameter import Parameter
from luigi import Task, ExternalTask, Event
import pandas as pd
import dask.dataframe as dd
from oandapyV20 import API
import oandapyV20.endpoints.transactions as transactions
from helperfiles.task import TargetOutput, Requirement, Requires
from helperfiles.target import ParquetTarget
#from helperfiles.task import TargetOutput, Requires, Requirement
#from helperfiles.target import ParquetTarget
from contextlib import suppress
from shutil import rmtree

# Todo: Add commenting
# Todo: Add parameter for limiting number of trades for downloading

# pipenv run luigi --module tradinghistory GetTradingHistory --local-scheduler

class MoveToArchieve(Task):
    if ParquetTarget(os.getenv('local_location') + 'trading_history/').exists() == False:
        def complete(self):
            return True

    if ParquetTarget(os.getenv('local_location') + 'trading_history/').exists():
        output = TargetOutput(os.getenv('local_location') + 'archive/', target_class=ParquetTarget)
        trading_history = ParquetTarget('./'+ os.getenv('local_location') + 'trading_history/')
        #TargetOutput(os.getenv('local_location') + 'trading_history/')
            #ParquetTarget('./'+ os.getenv('local_location') + 'trading_history/')

        def run(self):
            #if ParquetTarget(os.getenv('local_location') + 'trading_history/').exists():
            dsk = self.trading_history.read_dask()
            #with open(self.trading_history, 'r') as input_file:
            #with self.trading_history as input_file:
                #dsk = dd.read_parquet(input_file)
            self.output().write(dsk, write_metadata_file=True, compression='gzip')
            #with suppress(FileNotFoundError):
                #shutil.rmtree(os.getenv('local_location') + 'trading_history/', ignore_errors=True)






class S3(ExternalTask):
    output = TargetOutput(os.getenv('S3_location')+'tradinghistory/', target_class=ParquetTarget)


class DownloadS3(ExternalTask):
    requires = Requires()
    other = Requirement(S3)

    # Set output location
    output = TargetOutput(os.getenv('local_location')+'archive/', target_class=ParquetTarget)

    def run(self):
        input_target = next(iter(self.input().items()))[1]
        dsk = input_target.read()
        self.output().write(dsk)


class GetTradingHistory(Task):

    storage = Parameter(default='')

    client = API(access_token=os.getenv('TOKEN'), environment=os.getenv('OandaEnv'))

    if ParquetTarget(os.getenv('local_location') + 'archive/').exists():
        with suppress(FileNotFoundError):
            shutil.rmtree(os.getenv('local_location') + 'archive/', ignore_errors=True)

    #requires = Requires()
    #other = Requirement(MoveToArchieve)

    def requires(self):
        if self.storage == 's3':
            if ParquetTarget(os.getenv('S3_location')+'tradinghistory/').exists():
                return [DownloadS3()]
        return MoveToArchieve()

    output = TargetOutput('./'+ os.getenv('local_location')+ 'archive/', target_class=ParquetTarget)
    store = TargetOutput('./' + os.getenv('local_location') + 'trading_history/', target_class=ParquetTarget)
    s3store = TargetOutput(os.getenv('S3_location') + 'tradinghistory/', target_class=ParquetTarget)


    def gettransaction(self, first, last):
        trans = transactions.TransactionIDRange(accountID=os.getenv('ACCOUNT_ID'), params={"from": first, "to": last})
        trans = self.client.request(trans)

        return trans

    def run(self):
        last_trans = int(self.gettransaction(1, 2)['lastTransactionID'])
        pbar = tqdm(last_trans)

        if ParquetTarget('./'+ os.getenv('local_location') + 'archive/').exists():
            dsk = dd.read_parquet('./'+ os.getenv('local_location') + 'archive/*.parquet')
            #last_trans = 160000
        else:
            trans_df = self.gettransaction(1, 1000)
            df = pd.DataFrame(trans_df['transactions'])
            dsk = dd.from_pandas(df, chunksize=10000)
            #last_trans = 160000

        while int(dsk['id'].astype('int64').max().compute()) < last_trans:
            last_recorded = int(dsk['id'].astype('int64').max().compute())
            print(" - Reading history until id: {}".format(last_recorded))
            trans_df = self.gettransaction(last_recorded, last_recorded + 999)
            df = pd.DataFrame(trans_df['transactions'])
            # TODO: Improve this code
            for i in ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed','openTradeDividendAdjustments']:
                try:
                    df = df.drop(columns=i)
                except:
                    pass

            dsk = dd.concat([dsk, df])

            pbar.update(1000)

        # Todo: Rewrite
        for i in ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed','openTradeDividendAdjustments','shortPositionCloseout']:
            try:
                dsk = dsk.drop(i, axis =1)
            except:
                pass

        self.store().write(dsk, write_metadata_file=True, compression='gzip')

        if self.storage == 's3':
            self.s3store().write(dsk)
            print('Finished writing to S3')

    #@Task.event_handler(Event.SUCCESS)
    #def
    # remove_archive(self):
        #print('------- Slettet')

    #@Task.event_handler(Event.SUCCESS)
    #def remove_archive(task):
        #from time import sleep
        #sleep(3)
        #with suppress(FileNotFoundError):
            #shutil.rmtree(os.getenv('local_location') + 'archive/', ignore_errors=True)

            #print('------- Slettet')





