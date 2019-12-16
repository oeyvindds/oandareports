import os
import seaborn as sns
import matplotlib.pylab as plt
import dask.dataframe as dd
from luigi import Task
from ..helperfiles.task import TargetOutput, Requires, Requirement
from pandas.plotting import register_matplotlib_converters
from ..tools.tradinghistory import GetTradingHistory

register_matplotlib_converters()

""" This script extrqact finance charges and checks the account balance"""


class env_workaround:
    # Fix required for Travis CI
    def return_env(self, value):
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value


class FinancingReport(Task):

    requires = Requires()
    other = Requirement(GetTradingHistory)

    # Set output location
    output = TargetOutput(env_workaround().return_env("local_location") + "/image")

    df_list = []
    figs = []

    def create_graph(self, df):
        # Creating the graphs
        for i in ["accountBalance", "financing"]:
            ax = plt.gca()
            sns_plot = sns.lineplot(df.index, df[i])
            sns_plot.set_title("Running {}".format(i))
            sns_plot.set_ylabel("USD")
            plt.xticks(rotation=45)
            plt.tight_layout()
            fig = sns_plot.get_figure()
            if not os.path.exists(
                env_workaround().return_env("local_location") + "images/"
            ):
                os.makedirs(env_workaround().return_env("local_location") + "images/")
            fig.savefig(
                env_workaround().return_env("local_location")
                + "images/"
                + "{}.png".format(i)
            )
            self.figs.append(fig)

    def calculate(self, dsk):
        " Doea the calculations"
        dsk["time"] = dsk["time"].astype("M8[D]")
        dsk = dsk.set_index("time")
        dsk = dsk[dsk["type"].isin(["DAILY_FINANCING"])]
        dsk = dsk[["amount", "accountBalance", "financing"]]
        dsk["financing"] = dsk["financing"].fillna(0.0)
        dsk["financing"] = dsk["financing"].astype("float64")
        dsk["accountBalance"] = dsk["accountBalance"].astype("float64")
        df = dsk.compute()
        df["financing"] = df["financing"].cumsum(axis=0)
        return df

    def run(self):

        dsk = dd.read_parquet(
            env_workaround().return_env("local_location") + "trading_history/*.parquet"
        )
        df = self.calculate(dsk)
        self.create_graph(df)
