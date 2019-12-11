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

#class archive(Task):
    #shutil.rmtree(os.getcwd() + '/data/archive/', ignore_errors=True)
    #if ParquetTarget(os.getcwd() + '/data/trading_history/').exists():
   # os.rename(os.getcwd() + '/data/trading_history/', os.getcwd() + '/data/archive/')
    #os.mkdir(os.getcwd() + '/trading_history/')

    #shutil.rmtree('./' + os.getenv('local_location') + 'archive/', ignore_errors=True)
    #os.rename('./' + os.getenv('local_location') + 'trading_history/', './' + os.getenv('local_location') + 'archive/')
   # output = TargetOutput(os.getenv('local_location')+'archive/', target_class=ParquetTarget)

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


    #print('kvakk')
    #print(os.path.exists(os.getcwd() + '/data/trading_history/'))

    if os.path.exists(os.getcwd() + '/data/trading_history/'):
        #requires = Requires()
        #other = Requirement(archive)
        shutil.rmtree(os.getcwd() + '/data/archive/', ignore_errors=True)
        os.rename(os.getcwd() + '/data/trading_history/', os.getcwd() + '/data/archive/')
        #print('kvakk')


    #output = TargetOutput('./'+ os.getenv('local_location') + 'archive/', target_class=ParquetTarget)
    store = TargetOutput('./'+ os.getenv('local_location')+ 'trading_history/', target_class=ParquetTarget)
    s3store = TargetOutput(os.getenv('S3_location') + 'tradinghistory/', target_class=ParquetTarget)
    #if ParquetTarget('./' + os.getenv('local_location') + 'tradinghistory/').exists():
        #print('kvakk')

    def requires(self):
        if self.storage == 's3':
            if ParquetTarget(os.getenv('S3_location')+'tradinghistory/').exists():
                return [DownloadS3()]
        #else:
            #if ParquetTarget('./' + os.getenv('local_location') + 'tradinghistory/').exists():
                #print('kvakk2')
                #return archive()


    def gettransaction(self, first, last):
        trans = transactions.TransactionIDRange(accountID=os.getenv('ACCOUNT_ID'), params={"from": first, "to": last})
        trans = self.client.request(trans)

        return trans

    def run(self):
        last_trans = int(self.gettransaction(1, 2)['lastTransactionID'])
        pbar = tqdm(last_trans)
        #if ParquetTarget('./'+ os.getenv('local_location')+ 'trading_history/').exists():
            #requires = Requires()
            #other = Requirement(archive)
        if ParquetTarget('./'+ os.getenv('local_location') + 'archive/').exists():
            dsk = dd.read_parquet('./'+ os.getenv('local_location') + 'archive/*.parquet')
            last_trans = 50000
        else:
            trans_df = self.gettransaction(1, 1000)
            df = pd.DataFrame(trans_df['transactions'])
            dsk = dd.from_pandas(df, chunksize=10000)
            last_trans = 50000

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
        for i in ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed','openTradeDividendAdjustments']:
            try:
                dsk = dsk.drop(i, axis =1)
            except:
                pass

        #shutil.rmtree(os.getcwd() + '/data/archive/', ignore_errors=True)
        #if ParquetTarget('./' + os.getenv('local_location') + 'trading_history/').exists():
            #os.rename(os.getcwd() + '/data/trading_history/', os.getcwd() + '/data/archive/')
        #os.mkdir(os.getcwd() + 'trading_history/')
        #requires = Requires()
        #other = Requirement(archive)
        #os.mkdir(os.getcwd() + '/trading_history/')
        #if os.path.exists(os.getcwd() + '/data/trading_history/') == False:
            #os.mkdir(os.getcwd() + '/data/trading_history/')
        self.store().write(dsk)
        shutil.rmtree(os.getcwd() + '/data/archive/', ignore_errors=True)
        #dsk.to_parquet(os.getcwd() + '/data/trading_history/')

        if self.storage == 's3':
            self.s3store().write(dsk)
            shutil.rmtree('./'+ os.getenv('local_location') + 'archive/')
            print('Finished writing to S3')








