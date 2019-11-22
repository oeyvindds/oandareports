import os
from luigi.parameter import Parameter
from luigi import Task, ExternalTask
from oandareports.helperfiles.task import TargetOutput, Requires, Requirement
from oandareports.helperfiles.target import ParquetTarget


class ExistingHistoryS3(ExternalTask):
    """Ensure that the file exist"""

    output = TargetOutput("s3://advpython/oanda/tradinghistory/", target_class=ParquetTarget)

class ExistingHistoryLocal(ExternalTask):
    """Ensure that the file exist"""

    output = TargetOutput("data/", target_class=ParquetTarget)

class GetTradingHistory(Task):

    # Use parameter --storage s3 if output should be stored at ASW S3
    # If no history exist; use s3initial or initial for local
    storage = Parameter(default='')


    requires = Requires()

    # TODO: Fix parameter
    if storage == 's3':
        other = Requirement(ExistingHistoryS3)
        output = TargetOutput("s3://advpython/oanda/tradinghistory/", target_class=ParquetTarget)
    elif storage == 's3initial':
        output = TargetOutput("s3://advpython/oanda/tradinghistory/", target_class=ParquetTarget)
    elif storage == 'initial':
        if not os.path.isdir('data/'):
            os.mkdir('data/')
        output = TargetOutput("data/", target_class=ParquetTarget)
    else:
        print(storage)
        other = Requirement(ExistingHistoryLocal)
        output = TargetOutput("data/", target_class=ParquetTarget)

    def run(self):
        # As the input target is a dict, we need to get the item out
        input_target = next(iter(self.input().items()))[1]

        print(input_target)