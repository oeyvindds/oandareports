"""Tests for "Pset 4" package."""
import os

from unittest import TestCase
from unittest.mock import patch, MagicMock
from luigi import build, ExternalTask, format, Parameter, Task
from luigi.contrib.s3 import S3Target
from luigi.execution_summary import LuigiStatusCode
from luigi.mock import MockTarget

from csci_utils.luigi.target import SuffixPreservingLocalTarget

# from pset_4.tasks.data import ContentImage, SavedModel, DownloadImage, DownloadModel, UploadImage, UploadModel
# from pset_4.tasks.stylize import Stylize
# from pset_4.cli import main

IMAGE_ROOT = os.path.abspath('data/images')  # Path where model is saved
MODEL_ROOT = os.path.abspath('data/saved_models')  # Path where model is saved
S3_MODEL_ROOT = 's3://csci-USERNAME/pset_4/saved_models'
S3_IMAGE_ROOT = 's3://csci-USERNAME/pset_4/images'

IMAGE = 'fake_image.jpeg'
MODEL = 'fake_model.pth'

LOCAL_IMAGE_PATH = os.path.join(IMAGE_ROOT, IMAGE)
LOCAL_MODEL_PATH = os.path.join(MODEL_ROOT, MODEL)
S3_IMAGE_PATH = os.path.join(S3_IMAGE_ROOT, IMAGE)
S3_MODEL_PATH = os.path.join(S3_MODEL_ROOT, MODEL)


def build_func(func, **kwargs):
    return build([func(**kwargs)],
                 local_scheduler=True,
                 detailed_summary=True
                 ).status


class TasksDataTests(TestCase):
    def test_ContentImage_SavedModel(self):
        """Ensure ContentImage & SavedModel works correctly and as expected"""

        self.assertEqual(build_func(ContentImage, image=IMAGE), LuigiStatusCode.MISSING_EXT)
        self.assertEqual(build_func(SavedModel, model=MODEL), LuigiStatusCode.MISSING_EXT)

        with patch('csci_utils.luigi.target.SuffixPreservingLocalTarget.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(ContentImage, image=IMAGE), LuigiStatusCode.SUCCESS)
            self.assertEqual(build_func(SavedModel, model=MODEL), LuigiStatusCode.SUCCESS)
            SuffixPreservingLocalTarget.exists.assert_called()

    def test_UploadImage_UploadModel(self):
        """Ensure UploadImage and UploadModel works correctly and as expected"""

        self.assertEqual(build_func(UploadImage, image=IMAGE), LuigiStatusCode.MISSING_EXT)
        self.assertEqual(build_func(UploadModel, model=MODEL), LuigiStatusCode.MISSING_EXT)

        with patch('csci_utils.luigi.target.SuffixPreservingLocalTarget.exists', MagicMock(return_value=True)):
            with patch('csci_utils.luigi.target.SuffixPreservingLocalTarget.open', MagicMock()):
                self.assertEqual(build_func(UploadImage, image=IMAGE), LuigiStatusCode.FAILED)
                self.assertEqual(build_func(UploadModel, model=MODEL), LuigiStatusCode.FAILED)
                SuffixPreservingLocalTarget.exists.assert_called()
                SuffixPreservingLocalTarget.open.assert_called()

        with patch('luigi.contrib.s3.S3Target.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(UploadImage, image=IMAGE), LuigiStatusCode.SUCCESS)
            self.assertEqual(build_func(UploadModel, model=MODEL), LuigiStatusCode.SUCCESS)
            S3Target.exists.assert_called()

    def test_DownloadImage_DownloadModel(self):
        """Ensure DownloadImage & DownloadModel works correctly and as expected"""

        self.assertEqual(build_func(DownloadImage, image=IMAGE), LuigiStatusCode.MISSING_EXT)
        self.assertEqual(build_func(DownloadModel, model=MODEL), LuigiStatusCode.MISSING_EXT)

        with patch('luigi.contrib.s3.S3Target.exists', MagicMock(return_value=True)):
            with patch('luigi.contrib.s3.S3Target.open', MagicMock()):
                self.assertEqual(build_func(DownloadImage, image=IMAGE), LuigiStatusCode.FAILED)
                self.assertEqual(build_func(DownloadModel, model=MODEL), LuigiStatusCode.FAILED)
                S3Target.exists.assert_called()
                S3Target.open.assert_called()

        with patch('csci_utils.luigi.target.SuffixPreservingLocalTarget.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(DownloadImage, image=IMAGE), LuigiStatusCode.SUCCESS)
            self.assertEqual(build_func(DownloadModel, model=MODEL), LuigiStatusCode.SUCCESS)
            SuffixPreservingLocalTarget.exists.assert_called()


class TasksStylizeTests(TestCase):
    def test_Stylize(self):
        """Ensure Stylize functions correctly"""

        self.assertEqual(build_func(Stylize, image=IMAGE, model=MODEL), LuigiStatusCode.MISSING_EXT)

        with patch('luigi.contrib.s3.S3Target.exists', MagicMock(return_value=True)):
            self.assertEqual(build_func(Stylize, image=IMAGE, model=MODEL), LuigiStatusCode.FAILED)
            S3Target.exists.assert_called()

        with patch('csci_utils.luigi.target.SuffixPreservingLocalTarget.exists', MagicMock(return_value=True)):
            with patch('luigi.contrib.s3.S3Target.exists', MagicMock(return_value=True)):
                self.assertEqual(build_func(Stylize, image=IMAGE, model=MODEL), LuigiStatusCode.SUCCESS)
                SuffixPreservingLocalTarget.exists.assert_called()
                S3Target.exists.assert_called()

    def test_Stylize(self):
        """Ensure Run method of Stylize functions correctly"""

        class ContentI(ExternalTask):
            image = Parameter(default=IMAGE)

            def output(self):
                return MockTarget(self.image, format=format.Nop)

        class SavedM(ExternalTask):
            model = Parameter(default=MODEL)

            def output(self):
                return MockTarget(self.model, format=format.Nop)

        class NewStylize(Stylize):
            model = Parameter(default=MODEL)
            image = Parameter(default=IMAGE)

            def requires(self):
                return {
                    'image': self.clone(ContentI),
                    'model': self.clone(SavedM)
                }

            def output(self):
                return [MockTarget('fake_output.jpeg', format=format.Nop),
                        MockTarget('fake_output2.jpeg', format=format.Nop)]

        with patch('neural_style.neural_style.stylize', MagicMock(return_value=True)):
            mock_image = MockTarget(IMAGE, format=format.Nop)
            with mock_image.open('w') as f:
                f.write('abcd'.encode('utf'))

            mock_model = MockTarget(MODEL, format=format.Nop)
            with mock_model.open('w') as f:
                f.write('abcd'.encode('utf'))

            self.assertEqual(build_func(NewStylize), LuigiStatusCode.FAILED)

            mock_output_image = MockTarget('fake_output.jpeg', format=format.Nop)
            with mock_output_image.open('w') as f:
                f.write('abcd'.encode('utf'))

            mock_output_image2 = MockTarget('fake_output2.jpeg', format=format.Nop)
            with mock_output_image2.open('w') as f:
                f.write('abcd'.encode('utf'))

            self.assertEqual(build_func(NewStylize), LuigiStatusCode.SUCCESS)


class CommandLineInterFaceTests(TestCase):
    def test_CLI_Main(self):
        args = ['-i{}'.format(IMAGE), '-m{}'.format(MODEL)]
        self.assertEqual(main(args), LuigiStatusCode.MISSING_EXT)

        with patch('csci_utils.luigi.target.SuffixPreservingLocalTarget.exists', MagicMock(return_value=True)):
            with patch('luigi.contrib.s3.S3Target.exists', MagicMock(return_value=True)):
                self.assertEqual(main(args), LuigiStatusCode.SUCCESS)
                SuffixPreservingLocalTarget.exists.assert_called()
                S3Target.exists.assert_called()
