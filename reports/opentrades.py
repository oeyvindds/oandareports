import os
import matplotlib.pylab as plt
import dask.dataframe as dd
import numpy as np
from luigi import Task, build
from pandas.plotting import register_matplotlib_converters
from tools.historicrates import GetHistoricRates
import datetime as datetime
from helperfiles.task import Requires, Requirement
from tools.tradinghistory import GetTradingHistory

register_matplotlib_converters()

"""Reporting of open trades.
It gets the instruments from your trading history,
and creates a report based on this information"""


class OpenTradesReport(Task):
    def return_env(self, value):
        # Fix required for Travis CI
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value

    requires = Requires()
    other = Requirement(GetTradingHistory)
    figs = []

    def order_flow(self, dsk):
        # Read information and create graphs
        df = dsk.compute()
        for i in df["instrument"].unique():
            temp_df = df[df["instrument"] == i]
            temp_df = temp_df.copy()
            temp_df["transaction"] = np.where(temp_df.units > 0, 1, 0)
            task = build(
                [GetHistoricRates(instrument=i, granularity="H1")], local_scheduler=True
            )
            ddf_rate = dd.read_parquet(
                self.return_env("local_location")
                + "rates/"
                + i
                + "_"
                + "H1"
                + "/"
                + "part.*.parquet"
            )
            ddf_rate["time"] = dd.to_datetime(ddf_rate["time"])
            ddf_rate = ddf_rate[["c", "time"]]
            ddf_rate["close"] = ddf_rate["c"].astype("float64")
            ddf_rate = ddf_rate.compute()
            fig, ax = plt.subplots()
            scatter = ax.scatter(
                temp_df["time"], temp_df["price"], c=temp_df["transaction"]
            )
            ax.plot(ddf_rate["time"], ddf_rate["close"], alpha=0.4)
            plt.xlim(
                temp_df["time"].max() - datetime.timedelta(days=30),
                temp_df["time"].max(),
            )
            plt.xticks(rotation=45)
            plt.ylabel("USD")
            plt.title("Transactions for {}".format(i))
            plt.legend(handles=scatter.legend_elements()[0], labels=["Sell", "Buy"])
            if not os.path.exists(self.return_env("local_location") + "images/"):
                os.makedirs(self.return_env("local_location") + "images/")
            fig.savefig(
                self.return_env("local_location")
                + "images/"
                + "order_flow_{}.png".format(i)
            )
            self.figs.append(fig)

    def calculate(self, dsk):
        # Do the required calculations
        dsk["time"] = dsk["time"].astype("M8[D]")
        dsk = dsk[dsk["type"].isin(["ORDER_FILL"])]
        dsk = dsk[["time", "instrument", "units", "price"]]
        dsk["units"] = dsk["units"].astype("int64")
        dsk["price"] = dsk["price"].astype("float64")
        return dsk

    def run(self):
        dsk = dd.read_parquet(
            self.return_env("local_location") + "trading_history/*.parquet"
        )
        dsk = self.calculate(dsk)
        self.order_flow(dsk)
