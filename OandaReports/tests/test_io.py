#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for csci utils atomic write
All tests from Pset1 is implemented in addition to some new ones"""

import os
from tempfile import TemporaryDirectory
from unittest import TestCase

from csci_utils.io.io import atomic_write


class FakeFileFailure(IOError):
    pass


class AtomicWriteTests(TestCase):
    def test_atomic_write(self):
        """Ensure file exists after being written successfully"""
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "asdf.txt")

            with atomic_write(fp) as f:
                assert not os.path.exists(fp)
                tmpfile = f.name
                f.write("asdf")

            assert not os.path.exists(tmpfile)
            assert os.path.exists(fp)

            with open(fp) as f:
                self.assertEqual(f.read(), "asdf")

    def test_atomic_failure(self):
        """Ensure that file does not exist after failure during write"""
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "asdf.txt")

            with self.assertRaises(FakeFileFailure):
                with atomic_write(fp) as f:
                    tmpfile = f.name
                    assert os.path.exists(tmpfile)
                    raise FakeFileFailure()

            assert not os.path.exists(tmpfile)
            assert not os.path.exists(fp)

    def test_file_exists(self):
        """Ensure an error is raised when a file exist,
        and overwrite is not set to True"""
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "test.txt")
            with atomic_write(fp, mode="w", overwrite=True) as f:
                f.write("testing")
            with self.assertRaises(FileExistsError):
                with atomic_write(fp) as f:
                    f.write("testing")
            os.remove(fp)

    def test_double_suffixes(self):
        """Ensure an error is raised if atomic_write does not handle
        file-names with double suffixes, such as .tar.gz"""
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "asdf.tar.gz")
            with atomic_write(fp) as f:
                f.write("testing")
            assert os.path.exists(fp)
            os.remove(fp)

    def test_as_file(self):
        """Ensure that the atomic write can output the temporary
        path string if as_file = False"""
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "test.txt")
            with atomic_write(fp, mode="w", as_file=False) as f:
                pass

            assert isinstance(f, str)

    def test_mode(self):
        """Ensure that the atomic write can write other modes than 'w'"""
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "test.txt")
            with atomic_write(fp, mode="wb", as_file=True) as f:
                f.write(b"testing")

            with open(fp) as f:
                self.assertEqual(f.read(), "testing")

    def test_preserve_extension(self):
        """Ensure that the atomic write names the file with
        an 'unortodox' suffix aka preserves extension"""
        with atomic_write("test.dog", mode="w", as_file=True,
                          overwrite=True) as f:
            f.write("testing")

        with open("test.dog") as f:
            self.assertEqual(f.read(), "testing")

    def test_suffix(self):
        """Ensure that the atomic write can add suffix as needed"""
        with atomic_write(
            "test", mode="w", as_file=True, overwrite=True, suffix=".duck"
        ) as f:
            f.write("testing")

        with open("test.duck") as f:
            self.assertEqual(f.read(), "testing")

    @classmethod
    def tearDownClass(cls) -> None:
        """Not using a temporary file as we need to ensure the suffix is correct.
        Therefor cleaning up..."""
        os.remove("test.dog")
        os.remove("test.duck")
