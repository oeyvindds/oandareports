from unittest import TestCase
from luigi import ExternalTask, Task
from luigi.worker import Worker


class Dependency(TestCase):
    """Tests requirements of tasks"""

    def run(self, result=None):
        with Worker() as w:
            self.w = w
            super(Dependency, self).run(result)

    def test_dependency(self):
        class Volatility(ExternalTask):
            def complete(self):
                return False

        a = Volatility()

        class Exposure(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = Exposure()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)


class Dependency2(TestCase):
    """Tests that DownloadModel does require SavedModel
    as intended. Does also test that they both run as expected"""

    def run(self, result=None):
        with Worker() as w:
            self.w = w
            super(Dependency2, self).run(result)

    def test_modeldependency(self):
        class OpenTrades(ExternalTask):
            def complete(self):
                return False

        a = OpenTrades()

        class Financing(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = Financing()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)
