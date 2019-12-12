#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pset_3` package."""

import os
import sys
from io import StringIO
from contextlib import contextmanager
import numpy as np
import pandas as pd
from math import sqrt
from unittest import TestCase
# from pset_3.data import load_words, load_vectors, load_data
# from pset_3.embedding import WordEmbedding
# from pset_3.similarity import cosine_similarity
# from pset_3.cli import main, print_summary, user_project_vector, compute_distance_to_peers, compute_cosine_distances
from tempfile import TemporaryDirectory
from pyarrow.lib import ArrowIOError
from csci_utils.io.io import atomic_write


@contextmanager
def capture_print():
    _stdout = sys.stdout
    sys.stdout = StringIO()
    try:
        yield sys.stdout
    finally:
        sys.stdout = _stdout


class TestData(TestCase):

    def test_load_words(self):
        """basic test for load_words"""
        words = ["hello", "world", "this", "is", "pset3"]

        with TemporaryDirectory() as tmp:
            # write words to text file
            fp = os.path.join(tmp, "asdf.txt")
            with atomic_write(fp) as f:
                for w in words:
                    f.write(w + '\n')

            # ensure file exists
            assert os.path.exists(fp)

            # read words from file using load_words function
            actual_words = load_words(fp)

            # ensure expected and actual words match
            self.assertEqual(words, actual_words)

    def test_load_words_empty(self):
        """ensure loading words from an empty file returns an empty list"""
        with TemporaryDirectory() as tmp:
            # create empty text file
            fp = os.path.join(tmp, "asdf.txt")
            with atomic_write(fp):
                pass

            # ensure file exists
            assert os.path.exists(fp)

            # read words from file using load_words function
            actual_words = load_words(fp)

            # ensure expected and actual words match
            expected_words = []
            self.assertEqual(expected_words, actual_words)

    def test_load_words_missing_file(self):
        """ensure loading words from nonexistent file raises an error"""
        with TemporaryDirectory() as tmp:
            # path to nonexistent file
            fp = os.path.join(tmp, "asdf.txt")

            # ensure attempt to read words from nonexistent file raises an error
            self.assertRaises(FileNotFoundError, load_words, fp)

    def test_load_vectors(self):
        """basic test for load_vectors"""
        vectors = np.array([
            [2.7204e-01, -6.2030e-02, -1.8840e-01],
            [4.3285e-01, -1.0708e-01, 1.5006e-01],
            [-7.7706e-02, -1.3471e-01, 1.1900e-01]
        ])
        with TemporaryDirectory() as tmp:
            # write vectors to file
            fp = os.path.join(tmp, "asdf.npy")
            with atomic_write(fp, as_file=False) as f:
                np.save(f, vectors)

            # ensure numpy binary file exists
            assert os.path.exists(fp)

            # rename numpy binary file and ensure it exists
            os.rename(fp, fp+'.gz')
            fp += '.gz'
            assert os.path.exists(fp)

            # read vectors from file using load_vectors function
            actual_vectors = load_vectors(fp)

            # ensure expected and actual vectors match
            self.assertTrue(np.allclose(vectors, actual_vectors))

    def test_load_vectors_empty(self):
        """ensure loading vectors from an empty file raises an error"""
        with TemporaryDirectory() as tmp:
            # create empty file
            fp = os.path.join(tmp, "empty_h181k.npy")
            with atomic_write(fp, as_file=False) as f:
                np.save(f, np.array([]))

            # ensure file exists
            assert os.path.exists(fp)

            # rename numpy binary file and ensure it exists
            os.rename(fp, fp+'.gz')
            fp += '.gz'
            assert os.path.exists(fp)

            # load vectors
            actual_vectors = load_vectors(fp)

            # ensure expected and actual vectors match
            expected_vectors = np.array([])
            self.assertTrue(np.array_equal(expected_vectors, actual_vectors))

    def test_load_vectors_nonbinary(self):
        """ensure loading vectors from an empty nonbinary file raises an error"""
        with TemporaryDirectory() as tmp:
            # create empty file
            fp = os.path.join(tmp, "asdf.npy.gz")
            with atomic_write(fp):
                pass

            # ensure file exists
            assert os.path.exists(fp)

            # attempt to read vectors from file using load_vectors
            # and ensure it raises a value error
            self.assertRaises(ValueError, load_vectors, fp)

    def test_load_vectors_missing_file(self):
        """ensure loading vectors from nonexistent file raises an error"""
        with TemporaryDirectory() as tmp:
            # path to nonexistent file
            fp = os.path.join(tmp, "asdf.npy.gz")

            # ensure attempt to read vectors from nonexistent file raises an error
            self.assertRaises(FileNotFoundError, load_vectors, fp)

    def test_load_data(self):
        """basic test for data.load_data"""
        # create dataframe
        df = pd.DataFrame({'user_id': ['ffd3015f', '07ed8415', 'fffeeb82'],
                           'project': ['Two recent projects in python.\n\nThe first one.',
                                       'One python project involved a dashboard.',
                                       'I did work on Python script for automation.']})

        with TemporaryDirectory() as tmp:
            # write df to parquet file
            fp = os.path.join(tmp, "asdf.parquet")
            with atomic_write(fp, as_file=False) as f:
                df.to_parquet(f)

            # ensure file exists
            assert os.path.exists(fp)

            # read parquet file using load_data function
            actual_df = load_data(fp)

            # ensure expected and actual dataframes match
            self.assertTrue(df.equals(actual_df))

    def test_load_data_empty(self):
        """ensure loading data from an empty file returns an empty dataframe"""
        with TemporaryDirectory() as tmp:
            # create empty text file
            fp = os.path.join(tmp, "asdf.parquet")
            with atomic_write(fp):
                pass

            # ensure file exists
            assert os.path.exists(fp)

            # ensure expected and actual words match
            self.assertRaises(ArrowIOError, load_data, fp)

    def test_load_data_missing_file(self):
        """ensure loading data from nonexistent file raises an error"""

        with TemporaryDirectory() as tmp:
            # path to nonexistent file
            fp = os.path.join(tmp, "asdf.parquet")

            # ensure attempt to read nonexistent parquet file raises an error
            self.assertRaises(FileNotFoundError, load_data, fp)


class TestWordEmbedding(TestCase):

    def test_embeddding_from_data(self):
        """basic test to ensure WordEmbedding instance is generated correctly from data"""
        words = ['hello', 'world', 'csci']
        vectors = [[1.29, 9.2, 0.45], [7.32, 4.11, 2.8], [0.1, 3.7, 5.5]]
        embedding = WordEmbedding(words, vectors)

        # ensure embedding dict works
        self.assertTrue(np.allclose(embedding('hello'),[1.29, 9.2, 0.45]))
        self.assertTrue(np.allclose(embedding('world'), [7.32, 4.11, 2.8]))
        self.assertTrue(np.allclose(embedding('csci'), [0.1, 3.7, 5.5]))

    def test_embeddding_from_files(self):
        """basic test to ensure WordEmbedding instance is generated correctly from files"""
        words = ['csci', 'problem', 'set']
        vectors = [[7.32, 4.11, 2.8], [0.1, 3.7, 5.5], [1.29, 9.2, 0.45]]

        with TemporaryDirectory() as tmp:
            # write words to text file
            word_file = os.path.join(tmp, "asdf.txt")
            with atomic_write(word_file) as f:
                for w in words:
                    f.write(w + '\n')

            # write vectors to file
            vec_file = os.path.join(tmp, "asdf.npy")
            with atomic_write(vec_file, as_file=False) as f:
                np.save(f, vectors)
            os.rename(vec_file, vec_file+'.gz')
            vec_file += '.gz'

            # ensure files exists
            assert os.path.exists(word_file)
            assert os.path.exists(vec_file)

            # instantiate WordEmbedding from files
            embedding = WordEmbedding.from_files(word_file, vec_file)

            # ensure instance embedding is accurate
            self.assertTrue(np.allclose(embedding('csci'), [7.32, 4.11, 2.8]))
            self.assertTrue(np.allclose(embedding('problem'), [0.1, 3.7, 5.5]))
            self.assertTrue(np.allclose(embedding('set'), [1.29, 9.2, 0.45]))

    def test_tokenize(self):
        """ensure text can be tokenized into words"""
        # text to tokenize into words
        text = "Hello, I'm Scott"
        expected_tokens = ['hello', "i'm", 'scott']

        # create WordEmbedding instance
        words = ['python', 'scott', 'hello']
        vectors = [[1.3, 9.2, 0.45], [7.1, 4.11, 2.8], [0.3, 3.7, 5.5]]
        embedding = WordEmbedding(words, vectors)

        # tokenize text into words by invoking the tokenize function from the WordEmbedding instance
        actual_tokens = embedding.tokenize(text)

        # ensure actual and expected tokens match
        self.assertEqual(expected_tokens, actual_tokens)

    def test_embed_document(self):
        """ensure document embedding is correctly generated"""
        # create WordEmbedding instance
        words = ['this', 'is', 'my', 'python', 'dictionary']
        vectors = [[7.3, 4.1, 2.8], [1.3, 9.2, 0.4], [0.1, 3.7, 5.5], [0.8, 1.9, 2.2], [5.8, 4.7, 2.1]]
        embedding = WordEmbedding(words, vectors)

        # define document to embed
        text = 'Hello here is one python dictionary'
        actual_doc_embedding = embedding.embed_document(text)

        # ensure expected and actual doc embeddings match
        expected_doc_embedding = [7.9, 15.8, 4.7]
        self.assertTrue(np.allclose(expected_doc_embedding, actual_doc_embedding))

    def test_embed_document_words_not_in_dict(self):
        """ensure document embedding for text with no words in dict returns a vector of 0's"""
        # create WordEmbedding instance
        words = ['another', 'python', 'dictionary']
        vectors = [[1.3, 9.2, 0.4], [0.1, 3.7, 5.5], [5.8, 4.7, 2.1]]
        embedding = WordEmbedding(words, vectors)

        # define document to embed
        text = 'hello world this is csci'
        actual_doc_embedding = embedding.embed_document(text)

        # ensure expected and actual doc embeddings match
        expected_doc_embedding = np.zeros(3)
        self.assertTrue(np.allclose(expected_doc_embedding, actual_doc_embedding))


class TestSimilarity(TestCase):

    def test_cosine_similarity(self):
        """ensure cosine similarity is computed correctly"""
        cases = [
            # (expected, actual)
            (1/sqrt(2), cosine_similarity([0, 1], [1, 1])),  # vectors 45 degrees apart
            (0, cosine_similarity([0, 1], [1, 0])),          # orthogonal vectors
            (1, cosine_similarity([1, 1], [2, 2])),          # parallel vectors
            (-1, cosine_similarity([-1, -1], [1, 1])),       # anti-parallel vectors
            (1, cosine_similarity([1, 1], [1, 1])),          # identical vectors
        ]

        for expected, actual in cases:
            self.assertAlmostEqual(expected, actual)


    def test_cosine_similarity_division_by_zero(self):
        """ensure error is raised when at least one vector's magnitude is zero"""
        self.assertRaises(ZeroDivisionError, cosine_similarity, [0, 0], [1, 1])
        self.assertRaises(ZeroDivisionError, cosine_similarity, [1, 1], [0, 0])
        self.assertRaises(ZeroDivisionError, cosine_similarity, [0, 0], [0, 0])


class TestCli(TestCase):

    def test_print_summary(self):
        """ensure summary is printed in expected format"""
        df = pd.DataFrame({
            'user_id': ['j9d81dj9', 'md0m101d'],
            'distance': [0.5, 1.6],
            'project': ['project X', 'project Y']
        })

        expected = "User id: 0\nDistance: 0.5\nProject: project X\n--------------------------------------------------\n" \
                   + "User id: 1\nDistance: 1.6\nProject: project Y\n--------------------------------------------------\n"

        with capture_print() as std:
            print_summary(df)
        std.seek(0)
        captured = std.read()

        self.assertEqual(captured, expected)

    def test_user_project_vector(self):
        """ensure the correct user project vector is retrieved"""
        embeddings = pd.Series({
            'cam91lap': [0.72, 10.03, -22.53],
            '906bdb30': [1.9, 0.48, 9.12],
            'b0119mjk': [5.6, 2.71, -1.09]
        })
        user_name = 'John Doe'
        actual = user_project_vector(user_name, embeddings)
        self.assertTrue(np.allclose(actual, [1.9, 0.48, 9.12]))

    def test_compute_distance_to_peers(self):
        """ensure distance to peers is computed correctly"""
        embeddings = pd.Series({
            'cam91lap': [0, 1],
            '906bdb30': [1, 1],
            'b0119mjk': [-1, -1]
        })
        user_vector = [1, 1]
        expected_distance_to_peers = pd.Series({
            '906bdb30': 0.000000,
            'cam91lap': 1 - (1/sqrt(2)),
            'b0119mjk': 2,
        })
        expected_distance_to_peers.rename("distance", inplace=True)
        actual_distance_to_peers = compute_distance_to_peers(user_vector, embeddings)

        # ensure indices and values of pandas series are in the same order and equal
        self.assertEqual(expected_distance_to_peers.index.tolist(), actual_distance_to_peers.index.tolist())
        self.assertTrue(np.allclose(expected_distance_to_peers.tolist(), actual_distance_to_peers.tolist()))
