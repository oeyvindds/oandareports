import os
from luigi import Task, build
from csci_utils.luigi.task import TargetOutput, Requires, Requirement
from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from unittest import TestCase
import tempfile

"""Test the complete workflow of tasks.py, and confirm that the output is as expected"""


# TODO: Ikke oppdatert

class YelpReviews(Task):
    """Ensure that the file exist"""

    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write("review_id,user_id,business_id,stars,date,text,useful,funny,cool")
    tempFilePath = os.path.dirname(f.name) + "/"

    output = TargetOutput(tempFilePath, target_class=CSVTarget)


class CleanedReviews(Task):

    # Subset is the parameter for deciding if all or only partition one should be written
    # Include everything by setting parameter to --full
    # subset = Parameter()

    # Ensure requirements are in place
    requires = Requires()
    other = Requirement(YelpReviews)
    errorvar = False
    dsk = ""

    # Set output location
    output = TargetOutput("yelp_data/", target_class=ParquetTarget)

    def run(self):
        # Extract input from dictionary
        input_target = next(iter(self.input().items()))[1]

        # Extract the dask dataframe
        self.dsk = input_target.read()


class TestLuigiWorkflow(TestCase):
    def test_luigiflow(self):

        # Run the luigi workflow
        build([CleanedReviews()], local_scheduler=True)

        # Ensure the output is of the expected class, dask dataframe
        self.assertEqual(
            CleanedReviews.dsk.__class__, type("dask.dataframe.core.DataFrame")
        )
