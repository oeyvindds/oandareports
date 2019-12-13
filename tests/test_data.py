from unittest import TestCase
from luigi import ExternalTask, Task
from luigi.worker import Worker

# from pset_4.tasks.data import ContentImage, DownloadImage, SavedModel, DownloadModel


class ImageTests(TestCase):
    '''Tests that DownloadImage does require ContentImage
    as intended. Does also test that they both run as expected'''
    def run(self, result=None):
        with Worker() as w:
            self.w = w
            super(ImageTests, self).run(result)

    def test_dependency(self):
        class ContentImage(ExternalTask):
            def complete(self):
                return False

        a = ContentImage()

        class DownloadImage(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = DownloadImage()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

class ModelTests(TestCase):
    '''Tests that DownloadModel does require SavedModel
    as intended. Does also test that they both run as expected'''
    def run(self, result=None):
        with Worker() as w:
            self.w = w
            super(ModelTests, self).run(result)

    def test_modeldependency(self):
        class SavedModel(ExternalTask):
            def complete(self):
                return False

        a = SavedModel()

        class DownloadModel(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = DownloadModel()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

class TestData(TestCase):
    """Unit tests for the Required and downloading
    """

    def test01(self):
        """Test for model file that should return a Luigi Mock class
        """

        args = "--model {}".format("Test.pth")
        task = SavedModel(args)

        self.assertEqual(task.output().__module__, "luigi.mock")

    def test02(self):
        """Test for an model that does not exist"""

        args = "--model {}".format("Quack.pth")
        a = SavedModel(args)
        self.assertFalse(a.complete())

    def test03(self):
        """Test for image file that should return a Luigi Mock class
        """

        args = "--image {}".format("Test.jpg")
        task = ContentImage(args)

        self.assertEqual(task.output().__module__, "luigi.mock")

    def test04(self):
        """Test for an image-file that does not exist"""
        args = "--image {}".format("Quack.jpg")
        a = ContentImage(args)
        self.assertFalse(a.complete())

    def test05(self):
        """Test for model file that should return a Luigi Mock class
        """

        args = "--model {}".format("Test.pth")
        task = DownloadModel(args)

        self.assertEqual(task.output().__module__, "luigi.mock")

    def test06(self):
        """Test for an model that does not exist"""

        args = "--model {}".format("Quack.pth")
        a = DownloadModel(args)
        self.assertFalse(a.complete())

    def test07(self):
        """Test for image file that should return a Luigi Mock class
        """

        args = "--image {}".format("Test.jpg")
        task = DownloadImage(args)

        self.assertEqual(task.output().__module__, "luigi.mock")

    def test08(self):
        """Test for an image-file that does not exist"""

        args = "--image {}".format("Quack.jpg")
        a = DownloadImage(args)
        self.assertFalse(a.complete())

    def test09(self):
        """Ensuring that the ccsci_utils.luigi.target is used
        """

        args = "--image {}".format("luigi.jpg")
        task = DownloadImage(args)

        self.assertEqual(task.output().__module__, "csci_utils.luigi.target")

    def test10(self):
        """Ensuring that the ccsci_utils.luigi.target is used
        """

        args = "--model {}".format("rain_princess.pth")
        task = DownloadModel(args)

        self.assertEqual(task.output().__module__, "csci_utils.luigi.target")
