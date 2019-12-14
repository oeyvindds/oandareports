import boto3
from moto import mock_s3
from unittest import TestCase
import tempfile
from luigi.contrib.s3 import S3Client, S3Target

# Moto just need any keys
AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"


def create_bucket():
    """Will be created by moto"""
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="advpython")
    return conn


class TestS3Target(TestCase):
    """This class tests a number of S3-operations
    More specific testing of this homework's utilization
    of s3 is tested in the other files, such as
    test_luigi.py and test_data.py"""

    s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    tempFileContents = b"Content for testing S3."

    @mock_s3
    def setUp(self):
        """Generate a temporary file that can be tested"""
        f = tempfile.NamedTemporaryFile(mode="wb", delete=False)
        self.tempFilePath = f.name
        f.write(self.tempFileContents)

    @mock_s3
    def test_read(self):
        """Test to upload a file, then downloading it"""
        create_bucket()
        self.s3_client.put(self.tempFilePath, "s3://advpython/tempfile")
        t = S3Target("s3://advpython/tempfile", client=self.s3_client)
        read_file = t.open()
        file_str = read_file.read()
        self.assertEqual(self.tempFileContents, file_str.encode("utf-8"))

    @mock_s3
    def test_copy_directory(self):
        """
        Test copying a file
        """
        create_bucket()

        s3_dir = "s3://advpython/pset_5/yelp_data/"
        self.s3_client.mkdir(s3_dir)

        self.assertTrue(self.s3_client.exists(s3_dir))

        s3_dest = "s3://advpython/pset_5/new_yelp_data/"
        self.s3_client.mkdir(s3_dest)

        self.assertTrue(self.s3_client.exists(s3_dest))

        response = self.s3_client.copy(s3_dir, s3_dest)
        num, size = response

        self.assertIsInstance(response, tuple)
        self.assertGreaterEqual(num, 0)
        self.assertGreaterEqual(size, 0)

    @mock_s3
    def test_s3_path(self):
        """Test that bucket and key is extracted correctly"""
        self.assertEqual(
            ("advpython", "oanda"),
            S3Client._path_to_bucket_and_key("s3://advpython/oanda"),
        )