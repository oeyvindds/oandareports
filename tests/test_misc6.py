#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pset_3` package."""

import os
from tempfile import TemporaryDirectory
from unittest import TestCase

from csci_utils.io.io import atomic_write
import numpy as np
import pandas as pd

# from pset_3.data import load_data, load_vectors, load_words
# from pset_3.embedding import WordEmbedding
# from pset_3.utils import cosine_similarity


class DataTests(TestCase):
    # def test_load_words(self):
    #     """Ensure words are loaded from a text file"""
    #     words = ["word1", "word2", "word3"]
    #
    #     with TemporaryDirectory() as tmp:
    #         fp = os.path.join(tmp, "words.txt")
    #
    #         with atomic_write(fp, "w") as f:
    #             f.write("word1\n")
    #             f.write("word2\n")
    #             f.write("word3\n")
    #
    #         self.assertEqual(words, load_words(fp))

    def test_load_vectors(self):
        # """Ensure vectors are loaded from a numpy file"""
        vectors = np.array([[0, 1, 2], [3, 4, 5]])

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "vectors.npy")

            with atomic_write(fp, "w", as_file=False) as fname:
                np.save(fname, vectors)

            assert (vectors == load_vectors(fp)).all()

    def test_load_data(self):
        """Ensure data are loaded from a parquet file"""
        data = [["alice", "foo"], ["bob", "bar"]]
        df = pd.DataFrame(data, columns=["name", "desc"])
        df.set_index(df.name)

        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, "data.parquet")

            with atomic_write(fp, "w", as_file=False) as fname:
                df.to_parquet(fname, engine="pyarrow", index=True)

            assert df.equals(load_data(fp))


class EmbeddingTests(TestCase):
    def test_create_embedding_from_files(self):
        """Ensure embeddings can be loaded from files"""
        words = ["word"]
        vectors = np.array([[0, 1]])

        with TemporaryDirectory() as tmp:
            words_file = os.path.join(tmp, "words.txt")
            with atomic_write(words_file, "w") as f:
                f.write("word\n")

            vectors_file = os.path.join(tmp, "vectors.npy")
            with atomic_write(vectors_file, "w", as_file=False) as fname:
                np.save(fname, vectors)

            embedding = WordEmbedding.from_files(words_file, vectors_file)
            assert (embedding("word") == np.array([0, 1])).all()

    def test_embedding_in_vocab(self):
        """Ensure that the embedding of a word in the vocab is correct"""
        words = ["word"]
        vectors = np.array([[0, 1]])
        embedding = WordEmbedding(words, vectors)
        assert (embedding("word") == np.array([0, 1])).all()

    def test_embedding_out_of_vocab(self):
        """Ensure that the embedding of a word in the vocab is correct"""
        words = ["word"]
        vectors = np.array([[0, 1]])
        embedding = WordEmbedding(words, vectors)
        self.assertIsNone(embedding("other"))

    def test_embedding_diff_num_words_and_vectors(self):
        """Ensure an error occurs if passed invalid words and vectors"""
        words = ["word1", "word2"]
        vectors = np.array([[1, 2]])
        with self.assertRaises(ValueError):
            WordEmbedding(words, vectors)

    def test_embed_document_all_in_vocab(self):
        """Ensure that embedding documents with all valid words is correct"""
        words = ["i'm", "python", "writing"]
        vectors = np.array([[1, 2], [4, 8], [16, 32]])
        embedding = WordEmbedding(words, vectors)
        expected_document_embedding = np.array([21, 42])
        actual_embedding = embedding.embed_document("I'm writing Python.")
        assert (expected_document_embedding == actual_embedding).all()

    def test_embed_document_some_in_vocab(self):
        """Ensure that embedding documents with some valid words is correct"""
        words = ["i'm", "python", "writing"]
        vectors = np.array([[1, 2], [4, 8], [16, 32]])
        embedding = WordEmbedding(words, vectors)
        expected_document_embedding = np.array([17, 34])
        actual_embedding = embedding.embed_document("I'm writing Java.")
        assert (expected_document_embedding == actual_embedding).all()

    def test_embed_document_none_in_vocab(self):
        """Ensure that embedding documents with invalid words is all zeros"""
        words = ["cat", "hat", "in", "the"]
        vectors = np.array([[1, 2], [4, 8], [16, 32], [64, 128]])
        embedding = WordEmbedding(words, vectors)
        actual_embedding = embedding.embed_document("one fish two fish")
        assert (np.zeros(2) == actual_embedding).all()

    def test_embed_document_single_letters_not_tokenized(self):
        """Ensure that embedding documents doesn't tokenize one letter words"""
        words = ["i", "a"]
        vectors = np.array([[1, 2], [4, 8]])
        embedding = WordEmbedding(words, vectors)
        actual_embedding = embedding.embed_document("i am a student")
        assert (np.zeros(2) == actual_embedding).all()


class CosineSimilarityTests(TestCase):
    def test_cosine_similarity_same_direction_vectors(self):
        """"Ensure cosine similarity of vectors on same line is 1"""
        vec1 = np.array([0, 1])
        vec2 = np.array([0, 2])
        self.assertEqual(1, cosine_similarity(vec1, vec2))

    def test_cosine_similarity_perpendicular_vectors(self):
        """"Ensure cosine similarity of perpendicular vectors is 0"""
        vec1 = np.array([0, 1])
        vec2 = np.array([1, 0])
        self.assertEqual(0, cosine_similarity(vec1, vec2))

    def test_cosine_similarity_opposite_vectors(self):
        """"Ensure cosine similarity of opposite vectors is -1"""
        vec1 = np.array([0, 1])
        vec2 = np.array([0, -1])
        self.assertEqual(-1, cosine_similarity(vec1, vec2))

    def test_cosine_similarity_invalid_vectors(self):
        """"Ensure cosine similarity raises error for invalid vectors"""
        vec1 = np.array([[0, 1], [2, 3]])
        vec2 = np.array([0, -1])
        with self.assertRaises(ValueError):
            cosine_similarity(vec1, vec2)
