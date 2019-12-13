import os
import boto3
from csci_utils.luigi.dask.target import CSVTarget, ParquetTarget
from csci_utils.luigi.task import TargetOutput, Requires, Requirement
from luigi import BoolParameter

from luigi.contrib.s3 import S3Client
from moto import mock_s3
from unittest import TestCase
# from csci_utils.hash.hash_str import hash_str
from tempfile import NamedTemporaryFile, TemporaryDirectory

# from pset_5.tasks.yelp import YelpReviews, CleanedReviews, ByDecade, ByStars

class HashTests(TestCase):
    """
    Confirm utils hash_str
    """
    def test_basic(self):
        self.assertEqual(hash_str("world!", salt="hello, ").hex()[:6], "68e656")


AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


def create_bucket(bucket, dir, temp_file_path, download_path):
    """
    Create mock buckets for testing and put a test file in bucket
    at directory location if provided
    """
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)
    client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

    # if a directory was provided create corresponding path in the bucket
    if dir:
        client.mkdir("s3://{}/{}".format(bucket, dir))
        bucket_path = os.path.join(bucket, dir)
    else:
        bucket_path = bucket

    client.put(temp_file_path, "s3://{}/{}".format(bucket_path, download_path))


def create_temp_files():
    """
    Create a temporary file with contents and return
    tuple with file contents with file path
    :return: (file path, file contents)
    """
    f = NamedTemporaryFile(mode="wb", delete=False, suffix=".csv")
    tempFileContents = (
        b"review_id,user_id,business_id,stars,date,text,useful,funny,cool\n"
        + b"o1qYw1U8wGghaetGVFpvDQ,1,test-id,5,2017-10-14,It was good.,0,0,0\n"
        + b"o1qYw1U8wGghaetGVFpvDQ,1,test-id,5,2017-10-14,Was good and refreshing! I'll go again.,0,0,0\n"
        + b"yhEjeiKaIy8TWJL42Xgp1w,2,test-id,3,2013-07-18,Great Spot!,0,0,0"
    )
    tempFilePath = f.name
    f.write(tempFileContents)
    f.close()
    return tempFilePath, tempFileContents


def setupTest(self):
    # Get Temp File Path and Contents
    self.tempFilePath, self.tempFileContents = create_temp_files()
    # Ensure cleanup of file once complete
    self.addCleanup(os.remove, self.tempFilePath)


def get_mock_clean_reviews(tmp):
    """
    Create and return a MockCleanedReviews class
    :param tmp:
    :return:
    """
    # return class that will use provided tmp
    # folder as location for file
    class MockCleanedReviews(CleanedReviews):
        subset = BoolParameter(default=True)
        requires = Requires()
        reviews = Requirement(YelpReviews)
        output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=tmp + "/data/{task.__class__.__name__}-{task.subset}",
            ext="",
        )

    return MockCleanedReviews()


def get_dataframe(target):
    """
    Return a computed dataframe with it's rows and cols
    from the provided target
    :param target:
    :return:
    """
    target.run()
    target = target.output()
    df = target.read_dask()
    pdf = df.compute()
    rows, cols, = pdf.shape
    return rows, cols, pdf


def verify_dataframe_dimensions(self, target, rows, cols):
    """
    Confirm a given target has expected rows and cols and return
    resulting dataframe from target for further testing
    :param self:
    :param target:
    :param rows:
    :param cols:
    :return:
    """
    test_rows, test_cols, pdf = get_dataframe(target)
    self.assertEqual(test_rows, rows)
    self.assertEqual(test_cols, cols)
    return pdf


def verify_dataframe(self, tmp, rows, cols):
    """
    validate files in tmp location to ensure they
    contain the given number of rows and cols in total
    :param self:
    :param tmp:
    :param rows:
    :param cols:
    :return:
    """
    target = get_mock_clean_reviews(tmp)
    verify_dataframe_dimensions(self, target, rows, cols)


@mock_s3
class TestYelpReviews(TestCase):
    """
    Test Yelp Review retrieval from S3
    """
    def setUp(self):
        setupTest(self)

    def test_yelp_reviews(self):
        """
        Test Yelp is retrieved from mock S3 bucket
        :return:
        """
        tempFilePath, tempFileContent = create_temp_files()
        create_bucket(
            "ymdavis-csci-e-29", "pset_5/yelp_data", tempFilePath, "test-file.csv"
        )
        dm = YelpReviews()
        target = dm.output()
        self.assertTrue(isinstance(target, CSVTarget))
        df = target.read_dask()
        rows, cols = df.compute().shape
        self.assertEqual(rows, 3)
        self.assertEqual(cols, 9)


@mock_s3
class TestCleanedReviews(TestCase):
    """
    Test Cleaned Reviews
    """
    def setUp(self):
        setupTest(self)

    def create_error_files(self):
        """
        Create a temporary file with bad content in data
        :return: (file path, file contents)
        """
        f = NamedTemporaryFile(mode="wb", delete=False, suffix=".csv")
        tempFileContents = (
            b"review_id,user_id,business_id,stars,date,text,useful,funny,cool\n"
            + b"o1qYw1U8wGghaetGVF,1,test-id,5,2017-10-14,It was good.,0,0,0\n"
            + b"yhEjeiKaIy8TWJL42Xgp1w,2,test-id,5,2013-07-18,Great Spot!,0,0,0\n"
        )
        tempFilePath = f.name
        f.write(tempFileContents)
        f.close()
        return tempFilePath, tempFileContents

    def test_cleaned_reviews(self):
        """
        Test cleaned views were retrieved from bucked as expected
        :return:
        """
        tempFilePath, tempFileContent = create_temp_files()
        create_bucket(
            "ymdavis-csci-e-29", "pset_5/yelp_data", tempFilePath, "test-file.csv"
        )
        with TemporaryDirectory() as tmp:
            verify_dataframe(self,tmp, 3, 9)

    def test_cleaned_reviews_with_errors(self):
        """
         Test cleaned views were retrieved from bucket as expected
         with error rows removed
         :return:
         """

        tempFilePath, tempFileContent = self.create_error_files()
        create_bucket(
            "ymdavis-csci-e-29", "pset_5/yelp_data", tempFilePath, "test-file.csv"
        )
        with TemporaryDirectory() as tmp:
            verify_dataframe(self, tmp, 1, 9)


def get_mock_by_decade(clean, tmp):
    """
    Create and return a MockByDecade class
    :param tmp:
    :return:
    """
    # return class that will use provided tmp
    # folder as location for file
    class MockByDecade(ByDecade):
        subset = BoolParameter(default=True)
        requires = Requires()
        cleaned_reviews = Requirement(clean.__class__)
        output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=tmp + "/data/{task.__class__.__name__}-{task.subset}",
            ext="",
        )

    return MockByDecade()


def get_mock_by_stars(clean, tmp):
    """
    Create and return a MockByStars class
    :param tmp:
    :return:
    """
    # return class that will use provided tmp
    # folder as location for file
    class MockByStars(ByStars):
        subset = BoolParameter(default=True)
        requires = Requires()
        cleaned_reviews = Requirement(clean.__class__)
        output = TargetOutput(
            target_class=ParquetTarget,
            file_pattern=tmp + "/data/{task.__class__.__name__}-{task.subset}",
            ext="",
        )

    return MockByStars()


def get_clean_reviews_task(tmp):
    """
    Return a clean reviews task that has been executed
    :param tmp:
    :return:
    """
    clean = get_mock_clean_reviews(tmp)
    clean.run()
    return clean


@mock_s3
class TestByAggregates(TestCase):
    """
        Test aggregate tasks for average length
    """
    def setUp(self):
        setupTest(self)

    def test_by_decade(self):
        """
        test calculated average length by decade using temp file
        :return:
        """
        tempFilePath, tempFileContent = create_temp_files()
        create_bucket(
            "ymdavis-csci-e-29", "pset_5/yelp_data", tempFilePath, "test-file.csv"
        )
        with TemporaryDirectory() as tmp:
            target = get_mock_by_decade(get_clean_reviews_task(tmp), tmp)
            pdf = verify_dataframe_dimensions(self,target,1,1)
            self.assertEqual(pdf["length_reviews"].values[0], 21)

    def test_by_stars(self):
        """
        test calculated average length by star rating using temp file
        :return:
        """
        tempFilePath, tempFileContent = create_temp_files()
        create_bucket(
            "ymdavis-csci-e-29", "pset_5/yelp_data", tempFilePath, "test-file.csv"
        )
        with TemporaryDirectory() as tmp:
            target = get_mock_by_stars(get_clean_reviews_task(tmp), tmp)
            pdf = verify_dataframe_dimensions(self,target,2,1)
            self.assertEqual(pdf["length_reviews"].values[0], 11)
            self.assertEqual(pdf["length_reviews"].values[1], 26)
