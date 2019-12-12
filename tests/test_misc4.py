import os
import boto3
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest import TestCase

from luigi.contrib.s3 import S3Client
from moto import mock_s3
# from pset_4.tasks.data import (
#     ContentImage,
#     SavedModel,
#     DownloadModel,
#     DownloadImage,
#     SavedContent,
# )
# from pset_4.tasks.stylize import Stylize

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
    f = NamedTemporaryFile(mode="wb", delete=False)
    tempFileContents = (
        b"I'm a temporary file for testing\nAnd this is the second line\n"
        b"This is the third."
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


class MockStylize(Stylize):
    """
    Extend the Stylize class for testing. We don't want to test
    full neural style functionality so override program args so we can at least confirm
    the file name generated (which should prove workflow executed properly
    """

    def program_args(self):
        # Be sure to use self.temp_output_path
        return ["touch", self.temp_output_path]


@mock_s3
class ModelAndImageTest(TestCase):
    def setUp(self):
        setupTest(self)

    def confirm_content_output(self, root, download_file, s3_path):

        download_path = os.path.join(root, download_file)
        # Create bucket and store temp file as test download file
        create_bucket("mybucket", None, self.tempFilePath, download_path)

        if root == "images":
            # Create a ContentImage instance
            ci = ContentImage(root="s3://mybucket/", image=download_file)
        elif root == "saved_models":
            # Create a SavedModel instance
            ci = SavedModel(root="s3://mybucket/", model=download_file)
        else:
            ci = SavedContent(root="s3://mybucket/")

        test_s3 = ci.output()

        # Check that file object exists
        self.assertIsNotNone(test_s3)
        # Check that s3 file target path matches expected path
        self.assertEqual(test_s3.path, s3_path)

        # Retrieve contents and compare with temp file contents stored in setup
        with test_s3.open("r") as f:
            content = f.read()
        self.assertTrue(self.tempFileContents, content)

    def test_image_output(self):
        self.confirm_content_output(
            "images", "test-image.txt", "s3://mybucket/images/test-image.txt"
        )

    def test_model_output(self):
        self.confirm_content_output(
            "saved_models",
            "test-model.txt",
            "s3://mybucket/saved_models/test-model.txt",
        )

    def test_save_content_exception(self):
        with self.assertRaises(NotImplementedError):
            self.confirm_content_output("saved_content", "failed.text", "s3://failed/")


@mock_s3
class DownloadTest(TestCase):
    def setUp(self):
        setupTest(self)

    def download(self, path, download_file):

        download_path = os.path.join(path, download_file)
        # call creates a bucket with a path for images
        # and adds the temporary file file to bucket
        create_bucket("ymdavis-csci-e-29", "pset_4", self.tempFilePath, download_path)

        # Using a temporary directory to download load test file from mock
        # s3 and compare content
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, download_file)
            if path == "images":
                dm = DownloadImage(downloadFile=download_file, LOCAL_ROOT=tmp)
            else:
                dm = DownloadModel(downloadFile=download_file, LOCAL_ROOT=tmp)
            dm.run()
            self.assertTrue(os.path.exists(fp))
            with open(fp, "r") as f:
                content = f.read()
            self.assertTrue(self.tempFileContents, content)

    def test_download_model(self):
        downloadFile = "test-model.txt"
        self.download("saved_models", downloadFile)

    def test_download_image(self):
        downloadFile = "test-image.txt"
        self.download("images", downloadFile)


@mock_s3
class StylizeTest(TestCase):
    image_file = "test-image.txt"
    model_file = "test-model.txt"

    def setUp(self):

        # Create temporary files for test image and model
        self.imageTempFilePath, self.imageTempFileContents = create_temp_files()
        self.modelTempFilePath, self.modelTempFileContents = create_temp_files()

        image_download_path = os.path.join("images", self.image_file)
        model_download_path = os.path.join("saved_models", self.model_file)

        # Create bucket entries for files in mocked S3
        create_bucket(
            "ymdavis-csci-e-29", "pset_4", self.modelTempFilePath, model_download_path
        )
        create_bucket(
            "ymdavis-csci-e-29", "pset_4", self.imageTempFilePath, image_download_path
        )

        # register cleanup for temp files
        self.addCleanup(os.remove, self.imageTempFilePath)
        self.addCleanup(os.remove, self.modelTempFilePath)

    def test_stylize_via_object(self):
        """
        Execute run for Stylize and determine if output file name from process represents expected
        name
        """
        with TemporaryDirectory() as tmp:
            out_fp = os.path.join(tmp, "styled-test-model-test-image.txt")
            dm = MockStylize(
                image=self.image_file, model=self.model_file, LOCAL_ROOT=tmp
            )
            dm.run()
            self.assertTrue(os.path.exists(out_fp))
