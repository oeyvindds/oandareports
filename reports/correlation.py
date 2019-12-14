import os
import matplotlib.pylab as plt
#import pandas as pd
import dask.dataframe as dd
from luigi import Task, build
from luigi.parameter import Parameter
#from helperfiles.task import TargetOutput, Requires, Requirement
from tools.historicrates import GetHistoricRates
import seaborn as sns
import matplotlib.pyplot as plt
#from helperfiles.target import ParquetTarget
import datetime as datetime


class CorrelationReport(Task):
    granularity = Parameter()
    instruments = []

    def return_env(value):
        value = os.getenv(value)
        if value == None:
            value = 'not_availiable'
        return value

    def requires(self):
        return [build([GetHistoricRates(instrument=x, granularity=self.granularity)], local_scheduler=True) for x in self.instruments]

    def calculate(self, ddf, instrument):
        ddf = ddf[ddf['complete'] == True]
        ddf = ddf[['c','time']]
        ddf = ddf.astype({'c':'float64' })
        ddf = ddf.rename(columns={'c':instrument})
        ddf['time'] = dd.to_datetime(ddf['time'])
        ddf = ddf.set_index('time')
        return ddf


    def extract(self, instrument, granularity):
        ddf = dd.read_parquet(
            self.return_env('local_location') + 'rates/' + instrument + '_' + granularity + '/' + 'part.*.parquet')
        ddf = self.calculate(ddf, instrument)
        # ddf = ddf[ddf['complete'] == True]
        # ddf = ddf[['c','time']]
        # ddf = ddf.astype({'c':'float64' })
        # ddf = ddf.rename(columns={'c':instrument})
        # ddf['time'] = dd.to_datetime(ddf['time'])
        # ddf = ddf.set_index('time')
        return ddf


    def run(self):
        dsk = dd.read_parquet(self.return_env("local_location") + "trading_history/*.parquet")
        self.instruments = dsk['instrument'].drop_duplicates().compute()
        self.instruments = list(self.instruments.values)[1:]
        self.requires()
        a = self.extract(self.instruments.pop(), self.granularity)
        for i in self.instruments:
            b = self.extract(i,self.granularity)
            a = dd.concat([a,b], axis=1)
        fig, ax = plt.subplots(figsize=(12,7))
        sns_plot = sns.heatmap(a.corr(),
        xticklabels=a.columns,
        yticklabels=a.columns, annot=True, linewidths=.3)
        fig = sns_plot.get_figure()
        plt.title('Correlation of instruments in the portfolio with granularity {}'.format(self.granularity))
        name = 'correlation' + self.granularity + '.png'
        if not os.path.exists(self.return_env("local_location") + "images/"):
            os.makedirs(self.return_env("local_location") + "images/")
        fig.savefig(self.return_env('local_location') + 'images/' + name)
