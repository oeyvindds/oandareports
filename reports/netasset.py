import os
import matplotlib.pylab as plt
import dask.dataframe as dd
from luigi import Task, LocalTarget
from luigi.format import Nop
import datetime as datetime
from pylab import *
from contextlib import suppress
from helperfiles.task import TargetOutput, Requires, Requirement
from tools.tradinghistory import GetTradingHistory


class NetAssetReport(Task):
    def return_env(value):
        value = os.getenv(value)
        if value == None:
            value = 'not_availiable'
        return value

    requires = Requires()
    other = Requirement(GetTradingHistory)

    def output(self):
        # Remove old image, and replace it with a new one
        with suppress(FileNotFoundError):
            os.remove(self.return_env("local_location") + "images/" + "netassets.png")
        return LocalTarget(self.return_env("local_location") + "images/" + "netassets.png", format=Nop)

    def calculate(self, dsk):
        dsk = dsk[dsk.type.isin(["ORDER_FILL"])]
        dsk["time"] = dsk["time"].astype("M8[D]")
        dsk = dsk[["instrument", "time", "units", "price"]]
        dsk["price"] = dsk["price"].astype("float64")
        dsk["units"] = dsk["units"].astype("int64")
        dsk["cumsum"] = dsk.groupby(["instrument"])["units"].cumsum()
        dsk["value"] = abs(dsk["price"] * dsk["cumsum"])
        df = dsk.compute()
        return df


    def run(self):
        dsk = dd.read_parquet(self.return_env("local_location") + "trading_history/*.parquet")
        df = self.calculate(dsk)
        cmap = cm.get_cmap("tab20c", 15)
        fig, ax = plt.subplots(figsize=(10, 7))
        max_position = []
        for i, instrument in enumerate(df.instrument.unique()):
            df[df["instrument"] == instrument].plot.area(
                x="time",
                y="value",
                ax=ax,
                stacked=False,
                label=instrument,
                color=cmap(i),
            )
            max_position.append(df[df["instrument"] == instrument]['value'].max())

        plt.title("Total portfolio exposure")
        plt.ylabel("Exposure in USD")
        plt.ylim(0,(max(max_position)+max(max_position)*0.1))
        plt.xlim(df["time"].max() - datetime.timedelta(days=30), df["time"].max())
        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))
        with open(self.output().path, 'wb') as out_file:
            plt.savefig(out_file)
