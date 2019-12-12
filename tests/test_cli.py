import cli
from unittest import TestCase


class CliTests(TestCase):
    def test_cli(self):
        """Using cli.py to test for mock-file that does not exist.
        Functionality is also tested elsewhere. This is just for cli"""

        args = "--model {}".format("Test.pth")

        task = cli.main(args)

        self.assertEqual(task, True)