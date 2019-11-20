#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for csci utils hash_str"""

from unittest import TestCase

from csci_utils.hashing import hash_str, get_user_id, get_csci_salt


class HashTests(TestCase):
    def test_basic(self):
        """Ensure that the hash_str function gives the expected hash string"""
        self.assertEqual(hash_str("world!", salt="hello, ")
                         .hex()[:6], "68e656")

    def test_byte_hash(self):
        """Ensure that the hash_str function accepts salt already
        formatted as byte"""
        self.assertEqual(hash_str("world!", salt=b"hello, ")
                         .hex()[:6], "68e656")

    def test_username_type(self):
        """Ensure that an error gets raised if usernames in other
        formats than string is entered"""
        for n, expected in [(b"byte", TypeError), (123, TypeError)]:
            with self.assertRaises(TypeError):
                self.assertEqual(get_user_id(n), expected)

    def test_return_as_bytes(self):
        """Ensure that output from get_csci_salt is in byte format"""
        if not isinstance(get_csci_salt(), (bytes, bytearray)):
            raise ValueError()

    def test_username_type_num(self):
        """Ensure that a TypeError is raised if it is not fed a string"""
        with self.assertRaises(TypeError):
            self.assertEqual(get_user_id(1234), TypeError)
