import os
import luigi
from luigi import Task
from unittest import TestCase

"""Tests for luigi itself"""


class DummyTask(Task):
    """Create a dummy task for the tests below"""

    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run", self)
        self.has_run = True


class WorkerTest(TestCase):
    """Test tasks with dependence"""

    def test_dual_workers(self):
        class A(DummyTask):
            pass

        a = A()

        class B(DummyTask):
            def requires(self):
                return a

        ExternalB = luigi.task.externalize(B)
        b = B()
        eb = ExternalB()
        self.assertEqual(str(eb), "B()")


class ImportLuigiTest(TestCase):
    def test_import(self):
        """Test that Luigi is imported correctly"""

        installdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

        packagedir = os.path.join(installdir, "luigi")

        for root, dirs, files in os.walk(packagedir):
            package = os.path.relpath(root, luigidir).replace("/", ".")

            if "__init__.py" in files:
                __import__(package)

            for f in files:
                if f.endswith(".py") and not f.startswith("_"):
                    __import__(package + "." + f[:-3])

    def test_imports(self):
        import luigi

        testing = [
            luigi.Task,
            luigi.ExternalTask,
            luigi.LocalTarget,
            luigi.Parameter,
            luigi.build,
        ]
        self.assertGreater(len(testing), 0)
