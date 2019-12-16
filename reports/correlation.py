import os
import matplotlib.pylab as plt
import dask.dataframe as dd
from luigi import Task, build
from luigi.parameter import Parameter
from ..tools.historicrates import GetHistoricRates
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as datetime


class env_workaround:
    # Fix required for Travis CI
    def return_env(self, value):
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value


class CorrelationReport(Task):
    """This reports the correlation between the instruments in the portfolio
    :param granularity: The relevant Oanda time-granularity setting
                        Examples can be S5 for 5 seconds, M15 for 15 minutes and so forth
                        The 5000 last prices will be downloaded for the granularity, and
                        the correlation-report will be based of this
    """

    granularity = Parameter()
    instruments = []
    fig = object

    def requires(self):
        return [
            build(
                [GetHistoricRates(instrument=x, granularity=self.granularity)],
                local_scheduler=True,
            )
            for x in self.instruments
        ]

    def calculate(self, ddf, instrument):
        # The calculations for the report
        ddf = ddf[ddf["complete"] == True]
        ddf = ddf[["c", "time"]]
        ddf = ddf.astype({"c": "float64"})
        ddf = ddf.rename(columns={"c": instrument})
        ddf["time"] = dd.to_datetime(ddf["time"])
        ddf = ddf.set_index("time")
        return ddf

    def extract(self, instrument, granularity):
        ddf = dd.read_parquet(
            env_workaround().return_env("local_location")
            + "rates/"
            + instrument
            + "_"
            + granularity
            + "/"
            + "part.*.parquet"
        )
        ddf = self.calculate(ddf, instrument)
        return ddf

    def run(self):
        # The main function. Gets the script, creates graph and saves the result
        dsk = dd.read_parquet(
            env_workaround().return_env("local_location") + "trading_history/*.parquet"
        )
        self.instruments = dsk["instrument"].drop_duplicates().compute()
        self.instruments = list(self.instruments.values)[1:]
        self.requires()
        a = self.extract(self.instruments.pop(), self.granularity)
        for i in self.instruments:
            b = self.extract(i, self.granularity)
            a = dd.concat([a, b], axis=1)
        fig, ax = plt.subplots(figsize=(12, 7))
        sns_plot = sns.heatmap(
            a.corr(),
            xticklabels=a.columns,
            yticklabels=a.columns,
            annot=True,
            linewidths=0.3,
        )
        fig = sns_plot.get_figure()
        plt.title(
            "Correlation of instruments in the portfolio with granularity {}".format(
                self.granularity
            )
        )
        name = "correlation" + self.granularity + ".png"
        if not os.path.exists(
            env_workaround().return_env("local_location") + "images/"
        ):
            os.makedirs(env_workaround().return_env("local_location") + "images/")
        fig.savefig(env_workaround().return_env("local_location") + "images/" + name)
        self.fig = fig
