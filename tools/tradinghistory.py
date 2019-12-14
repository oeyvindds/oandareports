import os
import shutil
from tqdm import tqdm
from luigi.parameter import Parameter
from luigi import Task, ExternalTask, Event
import pandas as pd
import dask.dataframe as dd
from oandapyV20 import API
import oandapyV20.endpoints.transactions as transactions
from helperfiles.task import TargetOutput, Requirement, Requires
from helperfiles.target import ParquetTarget
from contextlib import suppress


"""
Functionality to download trading history
May be run like this: luigi --module tradinghistory GetTradingHistory --local-scheduler
or thru cli.py

Will download history from the account specified in .env
"""


class MoveToArchieve(Task):
    """This moves existing trading history into an archive folder.

    This is done so exisitng history, if existing may be read in and used.
    Hence, for transactions that are existing locally does not get read again.
    Then everything is written into the trading_history folder to ensure
    true atomic write"""

    local_location = os.getenv("local_location")
    if local_location == None:
        local_location = "data/"

    if ParquetTarget(local_location + "trading_history/").exists() == False:

        def complete(self):
            return True

    if ParquetTarget(local_location + "trading_history/").exists():
        output = TargetOutput(local_location + "archive/", target_class=ParquetTarget)
        trading_history = ParquetTarget(local_location + "trading_history/")

        def run(self):
            dsk = self.trading_history.read_dask()
            self.output().write(dsk, write_metadata_file=True, compression="gzip")


class env_workaround:
    # Fix required for Travis CI
    def return_env(self, value):
        value = os.getenv(value)
        if value == None:
            value = "not_availiable"
        return value


class S3(ExternalTask):
    # If -storage s3 is selected; everything gets stored at AWS S3 as backup

    output = TargetOutput(
        env_workaround().return_env("S3_location") + "tradinghistory/",
        target_class=ParquetTarget,
    )


class DownloadS3(ExternalTask):
    # Downloading from s3 if history exist there
    requires = Requires()
    other = Requirement(S3)

    # Set output location
    output = TargetOutput(
        env_workaround().return_env("local_location") + "archive/",
        target_class=ParquetTarget,
    )

    def run(self):
        input_target = next(iter(self.input().items()))[1]
        dsk = input_target.read()
        self.output().write(dsk)


class GetTradingHistory(Task):
    """
    The task that does the heavy lifting.
    Ensures to reuse existing history and append new.

    :param storage: s3 if you want backup to AWS s3
    :param max_transactions: The id of the latest event you want downloaded
                The account / token we have provided you with, contains about
                385 000 events. So a clean download will take some time, unless
                    this is set"""

    storage = Parameter(default="")
    max_transactions = Parameter(default=0)

    client = API(
        access_token=env_workaround().return_env("TOKEN"), environment="practice"
    )

    if ParquetTarget(
        env_workaround().return_env("local_location") + "archive/"
    ).exists():
        with suppress(FileNotFoundError):
            shutil.rmtree(
                env_workaround().return_env("local_location") + "archive/",
                ignore_errors=True,
            )

    def requires(self):
        if self.storage == "s3":
            if ParquetTarget(
                env_workaround().return_env("S3_location") + "tradinghistory/"
            ).exists():
                return [DownloadS3()]
        return MoveToArchieve()

    output = TargetOutput(
        "./" + env_workaround().return_env("local_location") + "archive/",
        target_class=ParquetTarget,
    )
    store = TargetOutput(
        "./" + env_workaround().return_env("local_location") + "trading_history/",
        target_class=ParquetTarget,
    )
    s3store = TargetOutput(
        env_workaround().return_env("S3_location") + "tradinghistory/",
        target_class=ParquetTarget,
    )

    def gettransaction(self, first, last):
        trans = transactions.TransactionIDRange(
            accountID=env_workaround().return_env("ACCOUNT_ID"),
            params={"from": first, "to": last},
        )
        trans = self.client.request(trans)

        return trans

    def run(self):
        # last_trans is the latest transaction on the server
        # May be overridden with max_transactions above
        last_trans = int(self.gettransaction(1, 2)["lastTransactionID"])
        pbar = tqdm(last_trans)

        if ParquetTarget(
            "./" + env_workaround().return_env("local_location") + "archive/"
        ).exists():
            dsk = dd.read_parquet(
                "./"
                + env_workaround().return_env("local_location")
                + "archive/*.parquet"
            )
            if self.max_transactions != 0:
                last_trans = self.max_transactions
        else:
            # If no local copy exist
            trans_df = self.gettransaction(1, 1000)
            df = pd.DataFrame(trans_df["transactions"])
            dsk = dd.from_pandas(df, chunksize=10000)
            if self.max_transactions != 0:
                last_trans = self.max_transactions

        while int(dsk["id"].astype("int64").max().compute()) < last_trans:
            last_recorded = int(dsk["id"].astype("int64").max().compute())
            print(" - Reading history until id: {}".format(last_recorded))
            trans_df = self.gettransaction(last_recorded, last_recorded + 999)
            df = pd.DataFrame(trans_df["transactions"])
            df = df.drop(
                columns=[
                    "takeProfitOnFill",
                    "fullPrice",
                    "tradeOpened",
                    "positionFinancings",
                    "tradeReduced",
                    "tradesClosed",
                    "openTradeDividendAdjustments",
                ],
                errors="ignore",
            )
            dsk = dd.concat([dsk, df])

            # The API only allows for 1000 events per request. Hence a progress bar
            pbar.update(1000)

        dsk = dsk.drop(
            columns=[
                "takeProfitOnFill",
                "fullPrice",
                "tradeOpened",
                "positionFinancings",
                "tradeReduced",
                "tradesClosed",
                "openTradeDividendAdjustments",
                "shortPositionCloseout",
            ],
            errors="ignore",
        )

        self.store().write(dsk, write_metadata_file=True, compression="gzip")

        if self.storage == "s3":
            self.s3store().write(dsk)
            print("Finished writing to S3")
