import os
import matplotlib.pylab as plt
import pandas as pd
import dask.dataframe as dd
from luigi import Task, build
from luigi.parameter import Parameter
from oandareports.helperfiles.task import TargetOutput, Requires, Requirement
from historicrates import GetHistoricRates
import seaborn as sns
import matplotlib.pyplot as plt
from oandareports.helperfiles.target import ParquetTarget
import datetime as datetime


class CorrelationReport(Task):
    granularity = Parameter()
    instruments = []

    def requires(self):
        return [build([GetHistoricRates(instrument=x, granularity=self.granularity)], local_scheduler=True) for x in self.instruments]
        #return [
            #GetHistoricRates(instrument='USD_NOK', granularity='D'),
            #GetHistoricRates(instrument='USD_SEK', granularity='D')
        #]
    #requires = Requires()
    #other = Requirement(GetHistoricRates(instrument='USD_NOK', granularity='D'))
    #other2 = Requirement(GetHistoricRates(instrument='USD_SEK', granularity='D'))

    #def requires(self):
        #task = build([GetHistoricRates(instrument=self.instrument[0], granularity='D')], local_scheduler=True)
        #task = build([GetHistoricRates(instrument=self.instrument[0], granularity='W')], local_scheduler=True)

    #output = TargetOutput('./'+ os.getenv('local_location')+ 'trading_history2/', target_class=ParquetTarget)

    def extract(self, instrument, granularity):
        ddf = dd.read_parquet(
            os.getenv('local_location') + 'rates/' + instrument + '_' + granularity + '/' + 'part.*.parquet')
        ddf = ddf[ddf['complete'] == True]
        ddf = ddf[['c','time']]
        ddf = ddf.astype({'c':'float64' })
        ddf = ddf.rename(columns={'c':instrument})
        ddf['time'] = dd.to_datetime(ddf['time'])
        ddf = ddf.set_index('time')
        return ddf


    def run(self):
        dsk = dd.read_parquet(os.getenv("local_location") + "trading_history/*.parquet")
        self.instruments = dsk['instrument'].drop_duplicates().compute()
        self.instruments = list(self.instruments.values)[1:]
        self.requires()
        a = self.extract(self.instruments.pop(), self.granularity)
        a['time'] = dd.to_datetime(a['time'])
        a = a.set_index('time')
        for i in self.instruments:
            b = self.extract(i,'S5')
            a = dd.concat([a,b])
        print(a.compute())
        a.compute().to_csv('temp.csv')
        fig, ax = plt.subplots(figsize=(12,7))
        sns.heatmap(a)
        #fig = sns.get_figure()
        #name = instrument + '_' + 'exposure.png'
        #TODO: Make directory if not existing
        fig.savefig(os.getenv('local_location') + 'images/' + 'correlation.png')
        fig.clf()








        # dsk = dsk[dsk.type.isin(["ORDER_FILL"])]
        # dsk["time"] = dsk["time"].astype("M8[D]")
        # dsk = dsk[["instrument", "time", "units", "price"]]
        # dsk["price"] = dsk["price"].astype("float64")
        # dsk["units"] = dsk["units"].astype("int64")
        # dsk["cumsum"] = dsk.groupby(["instrument"])["units"].cumsum()
        # dsk["value"] = abs(dsk["price"] * dsk["cumsum"])
        # df = dsk.compute()
        # cmap = cm.get_cmap("tab20c", 15)
        # fig, ax = plt.subplots(figsize=(10, 7))
        # for i, instrument in enumerate(df.instrument.unique()):
        #     df[df["instrument"] == instrument].plot.area(
        #         x="time",
        #         y="value",
        #         ax=ax,
        #         stacked=False,
        #         label=instrument,
        #         color=cmap(i),
        #     )
        #
        # plt.title("Total portfolio exposure")
        # plt.ylabel("Exposure in USD")
        # plt.xlim(df["time"].max() - datetime.timedelta(days=30), df["time"].max())
        # plt.savefig(os.getenv("local_location") + "images/" + "netassets.png")