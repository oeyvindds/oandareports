import os
import seaborn as sns
import matplotlib.pylab as plt
from luigi import Task
from helperfiles.task import TargetOutput, Requires, Requirement
from helperfiles.target import ParquetTarget
from tools.tradinghistory import GetTradingHistory

""" This script checks your trading history and extract the exposure you have (and had) for each instrument"""


class env_workaround:
    # Fix required for Travis CI
    def return_env(self, value):
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value


class ExposureReport(Task):
    # Get trading history
    requires = Requires()
    other = Requirement(GetTradingHistory)

    # Set output location
    output = TargetOutput(
        "../" + env_workaround().return_env("local_location") + "reports/",
        target_class=ParquetTarget,
    )

    df_list = []
    # Placeholder for plot
    fig = object

    def calculate(self, dsk):
        # Do the necessary calculations
        dsk["units"] = dsk["units"].astype("int64")
        dsk["time"] = dsk["time"].astype("M8[h]")
        df = dsk.compute()
        df["cum_sum"] = df["units"].cumsum()
        return df

    def create_graph(self, instrument, dsk):
        # Create the graphs
        dsk = dsk[dsk.instrument == instrument]
        df = self.calculate(dsk)
        ax = plt.gca()
        sns_plot = sns.lineplot(df["time"], df["cum_sum"], estimator=None)
        sns_plot.set_title("Cummulative exposure in {} units".format(instrument))
        sns_plot.set_ylabel("Cummulative units")
        sns_plot.set_xlabel("Timeframe")
        plt.xticks(rotation=45)
        plt.tight_layout()
        fig = sns_plot.get_figure()
        name = instrument + "_" + "exposure.png"
        if not os.path.exists(
            env_workaround().return_env("local_location") + "images/"
        ):
            os.makedirs(env_workaround().return_env("local_location") + "images/")
        fig.savefig(env_workaround().return_env("local_location") + "images/" + name)

        # return the object to the caller
        self.fig = fig

    def run(self):
        # Save the graphs
        input_target = next(iter(self.input().items()))[1]

        dsk = input_target.read()

        dsk = dsk.drop(
            columns=[
                "homeCurrency",
                "accountUserID",
                "batchID",
                "orderID",
                "userID",
                "accountID",
                "tradeID",
                "pl",
                "requestID",
                "gainQuoteHomeConversionFactor",
                "requestedUnits",
                "fundingReason",
                "timeInForce",
                "positionFill",
                "triggerCondition",
                "partialFill",
                "alias",
                "accountNumber",
                "divisionID",
                "siteID",
                "commission",
                "rejectReason",
                "accountFinancingMode",
                "financing",
                "marginRate",
                "guaranteedExecutionFee",
                "lossQuoteHomeConversionFactor",
                "halfSpreadCost",
                "fullVWAP",
            ],
            errors="ignore",
        )
        dsk = dsk[dsk.type.isin(["ORDER_FILL"])]

        for i in dsk["instrument"].unique().compute():
            print(i)
            self.create_graph(i, dsk)
