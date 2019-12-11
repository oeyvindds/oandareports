import os
import matplotlib.pylab as plt
import dask.dataframe as dd
from luigi import Task
import datetime as datetime
from pylab import *


class NetAssetReport(Task):
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
        for i, instrument in enumerate(df.instrument.unique()):
            df[df["instrument"] == instrument].plot.area(
                x="time",
                y="value",
                ax=ax,
                stacked=False,
                label=instrument,
                color=cmap(i),
            )

        plt.title("Total portfolio exposure")
        plt.ylabel("Exposure in USD")
        plt.xlim(df["time"].max() - datetime.timedelta(days=30), df["time"].max())
        plt.savefig(os.getenv("local_location") + "images/" + "netassets.png")
