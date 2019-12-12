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

    requires = Requires()
    other = Requirement(GetTradingHistory)

    def output(self):
        # Remove old image, and replace it with a new one
        with suppress(FileNotFoundError):
            os.remove(os.getenv("local_location") + "images/" + "netassets.png")
        return LocalTarget(os.getenv("local_location") + "images/" + "netassets.png", format=Nop)

    def run(self):
        dsk = dd.read_parquet(os.getenv("local_location") + "trading_history/*.parquet")
        dsk = dsk[dsk.type.isin(["ORDER_FILL"])]
        dsk["time"] = dsk["time"].astype("M8[D]")
        dsk = dsk[["instrument", "time", "units", "price"]]
        dsk["price"] = dsk["price"].astype("float64")
        dsk["units"] = dsk["units"].astype("int64")
        dsk["cumsum"] = dsk.groupby(["instrument"])["units"].cumsum()
        dsk["value"] = abs(dsk["price"] * dsk["cumsum"])
        df = dsk.compute()
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
        #print(max_position)
        plt.ylim(0,(max(max_position)+max(max_position)*0.1))
        plt.xlim(df["time"].max() - datetime.timedelta(days=30), df["time"].max())
        #image = plt.savefig(os.getenv("local_location") + "images/" + "netassets.png")
        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))
        with open(self.output().path, 'wb') as out_file:
            plt.savefig(out_file)
