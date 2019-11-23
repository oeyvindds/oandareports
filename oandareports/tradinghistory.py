import os
import shutil
from tqdm import tqdm
from luigi.local_target import LocalFileSystem, FileSystemTarget
from luigi.parameter import Parameter, OptionalParameter
from luigi import Task, ExternalTask
import pandas as pd
import dask.bag as db
import dask.dataframe as dd
import oandapyV20
from oandapyV20 import API
import oandapyV20.endpoints.transactions as transactions
from oandareports.helperfiles.task import TargetOutput, Requires, Requirement
from oandareports.helperfiles.target import ParquetTarget

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

    # Use parameter --storage s3 if output should be stored at ASW S3
    # If no history exist; use s3initial or initial for local
    storage = Parameter(default='')
    #initial = OptionalParameter(default=False)

    client = API(access_token=os.getenv('TOKEN'))

    output = TargetOutput(os.getenv('local_location') + 'archive/', target_class=ParquetTarget)
    store = TargetOutput(os.getenv('local_location'), target_class=ParquetTarget)

    #requires = Requires()

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
        #return pd.DataFrame(trans['transactions']), trans['lastTransactionID']
        return trans

    def run(self):
        last_trans = int(self.gettransaction(1, 2)['lastTransactionID'])
        pbar = tqdm(last_trans)
        if ParquetTarget(os.getenv('local_location') + 'archive/').exists():
            input_target = next(iter(self.input()))
            dsk = input_target.read()
            last_trans = 10000
        else:
            trans_df = self.gettransaction(1, 1000)
            df = pd.DataFrame(trans_df['transactions'])
            dsk = dd.from_pandas(df, chunksize=10000)
            last_trans = 10000

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
            #print(int(df['id'].astype('int64').max()))
            dsk = dd.concat([dsk, df])
            #dsk = dsk.merge(df, how='left', left_on=leftcolumns, right_on=rightcolumns)
            #print(len(dsk)
            pbar.update(1000)
            #dsk_list.append(df)

        #dsk = dd.from_delayed(dsk_list)
        # TODO: Improve this code
        #for i in ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed']:
            #try:
                #df = dsk.drop(i, axis=1)
            #except:
                #pass
        dsk = dsk.drop(
             ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed'],
             axis=1)
        #print(len(dsk))
        self.store().write(dsk)
        #shutil.rmtree(os.getenv('local_location') + 'archive/')
        #print(dsk.compute())
        #dsk = dsk.to_csv('temp.csv')







        #print(self.input())
        #if ParquetTarget(os.getenv('local_location') + 'archive/').exists():
            #input_target = next(iter(self.input()))
            #dsk = input_target.read()
        #else:

        # TODO: Fix so Dask accepts more than the first columns
        #trans_df = db.from_sequence(trans_df['transactions'])
        #trans_df = trans_df.to_dataframe()

        #trans_df = trans_df.fillna('-')
        #trans_df = trans_df.to_csv('temp.csv')

        #trans_df = pd.DataFrame(trans_df['transactions'])

        #self.store().write(trans_df)
        #trans_df = trans_df.to_csv('temp.csv')





        #print(dsk)



            # if self.initial == 'true':
            #     trans_df = self.gettransaction(1, 1000)
            #     last_trans = int(trans_df['lastTransactionID'])
            #     trans_df = pd.DataFrame(trans_df['transactions'])
            #
            #     last_trans = 6000
            #     pbar = tqdm(last_trans)
            #     # print(int(trans_df['id'].astype('int64').max()))
            #
            #     while int(trans_df['id'].astype('int64').max()) < last_trans:
            #         temp_df = pd.DataFrame(self.gettransaction(trans_df['id'].astype('int64').max(),
            #                                                    trans_df['id'].astype('int64').max() + 999)[
            #                                    'transactions'])
            #         trans_df = pd.concat([trans_df, temp_df])
            #         pbar.update(1000)
            #
            # trans_df = dd.from_pandas(trans_df, chunksize=1000)
            # trans_df = trans_df.drop(
            #     ['takeProfitOnFill', 'fullPrice', 'tradeOpened', 'positionFinancings', 'tradeReduced', 'tradesClosed'],
            #     axis=1)
            # self.output().write(trans_df)


            #else:
                #pass
                #requires = Requires()

            #return ExistingHistoryS3()
        #elif self.initial == False:
            #return ExistingHistoryLocal()
            #return TargetOutput("data/", target_class=ParquetTarget)

    #def output(self):
        #if self.storage == 's3':
            #ParquetTarget(os.getenv('S3_location')+'tradinghistory/')
            #return TargetOutput("s3://advpython/oanda/tradinghistory/", target_class=ParquetTarget)
        #else:
            #return ParquetTarget(os.getenv('local_location'))
            #TargetOutput("data/", target_class=ParquetTarget)

    #requires = Requires()

    #elif storage == 's3initial':
        #output = TargetOutput("s3://advpython/oanda/tradinghistory/", target_class=ParquetTarget)
    #elif storage == 'initial':
        #if not os.path.isdir('data/'):
            #os.mkdir('data/')
        #output = TargetOutput("data/", target_class=ParquetTarget)
    #else:
        #print(storage)
        #other = Requirement(ExistingHistoryLocal)
        #output = TargetOutput("data/", target_class=ParquetTarget)






