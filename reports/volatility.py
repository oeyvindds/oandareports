import os
import seaborn as sns
import matplotlib.pylab as plt

import dask.dataframe as dd
from luigi import build
from luigi.parameter import Parameter
from luigi import Task
from ..tools.historicrates import GetHistoricRates


class VolatilityReport(Task):
    """This function create the volatility report for a certain instrument
        By default it creates to graphs; one with a daily granularity and one with a weekly.
        If you have different requirements, it may obviously be changed below.
        But for most reasonable tasks, I believe daily + weekly should be ideal

        :param instrument: The instrument you want reports for. Could be USD_BCO
        """

    def return_env(value):
        # Fix required for Travis CI
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value

    instrument = Parameter()
    figs = []

    def requires(self):
        # Utilize the task from the tools to get the historic prices
        task = build(
            [GetHistoricRates(instrument=self.instrument, granularity="D")],
            local_scheduler=True,
        )
        task = build(
            [GetHistoricRates(instrument=self.instrument, granularity="W")],
            local_scheduler=True,
        )

    def add_arguments(self, parser):
        # The instrument it should create report for
        parser.add_argument("--instrument", nargs="2", type=str)

    def calculate(self, ddf):
        # Manipulate the dataframe
        ddf = ddf.astype(
            {
                "complete": "bool",
                "volume": "int64",
                "o": "float64",
                "h": "float64",
                "l": "float64",
                "c": "float64",
            }
        )
        ddf = ddf[ddf["complete"] == True]
        ddf["time"] = dd.to_datetime(ddf["time"])
        ddf = ddf.set_index("time")
        ddf["vol"] = ddf["h"] - ddf["l"]
        ddf["mov"] = ddf["c"] - ddf["o"]
        pdf = ddf.compute()
        return pdf

    def analyze(self, granularity):
        # Create graphs
        ddf = dd.read_parquet(
            self.return_env("local_location")
            + "rates/"
            + str(self.instrument)
            + "_"
            + granularity
            + "/"
            + "part.*.parquet"
        )
        pdf = self.calculate(ddf)
        ax = sns.jointplot(pdf["mov"], pdf["vol"], alpha=0.2)
        ax.set_axis_labels("Low to high", "Open to close", fontsize=14)
        if granularity == "D":
            plt.title("Daily volatility {}".format(self.instrument), fontsize=14)
            name = "daily_volatility_{}.png".format(self.instrument)
        else:
            plt.title("Weekly volatility {}".format(self.instrument), fontsize=14)
            name = "weekly_volatility_{}.png".format(self.instrument)
        plt.tight_layout()
        if not os.path.exists(self.return_env("local_location") + "images/"):
            os.makedirs(self.return_env("local_location") + "images/")
        ax.savefig(self.return_env("local_location") + "images/" + name)
        self.figs.append(ax)

    def run(self, *args, **options):
        # Change D and W here to whatever you want, if you need another timeframe
        day = self.analyze("D")
        week = self.analyze("W")
