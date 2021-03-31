#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Tatoeba Project, free collaborative creation of multilingual corpuses project
# Copyright (C) 2012 Allan SIMON <allan.simon@supinfo.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#
# @category Tatodetect
# @package  Tools
# @author   Allan SIMON <allan.simon@supinfo.com>
# @license  Affero General Public License
# @link     http://tatoeba.org
#
import codecs
import os
import sqlite3
import sys
from collections import defaultdict

# languages that don't use an alphabet
# we put them apart as they are likely to have a lot of different
# ngrams and by so need to have a lower limit for the ngrams we kept
# for that languages
IDEOGRAM_LANGS = frozenset(["wuu", "yue", "cmn"])
IDEOGRAM_NGRAM_FREQ_LIMIT = 0.000005
NGRAM_FREQ_LIMIT = 0.00001
# number of 1-gram a user must have submitted in one language to
# be considered as possibly contributing in that languages
# NOTE: this number is currently purely arbitrary
USR_LANG_LIMIT = 400
# we will generate the ngram from 2-gram to X-grams
UP_TO_N_GRAM = 5
# some names of the table in the database
TABLE_NGRAM = "grams"
TABLE_STAT = "langstat"
TABLE_USR_STAT = "users_langs"
INSERT_NGRAM = "INSERT INTO %s VALUES (?,?,?,?);"
INSERT_USR_STAT = "INSERT INTO %s VALUES (?,?,?);"


def generate_db(database_path):
    """Create the database and all the required tables

    Parameters
    ----------
    database_path : str
        the path of the Tatodetect sqlite database
    """
    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    for size in range(2, UP_TO_N_GRAM + 1):
        table = TABLE_NGRAM + str(size)
        c.execute(
            """
            CREATE TABLE %s (
             'gram' text not null,
             'lang'text not null,
             'hit'  int not null,
             'percent' float not null default 0
            );
            """
            % (table)
        )
    conn.commit()

    c.execute(
        """
        CREATE TABLE %s (
            'user' text not null,
            'lang' text not null ,
            'total' int not null default 0
        );
        """
        % (TABLE_USR_STAT)
    )
    conn.commit()
    c.close()


def generate_n_grams(database_path, sentences_path, tags_path):
    """Count the occurrences of the ngrams in the Tatoeba corpora

    Parameters
    ----------
    database_path : str
        the path of the Tatodetect sqlite database
    sentences : str
        the path of the Tatoeba 'sentences_detailed.csv' datafile
    tags_path : str
        the path of the Tatoeba tags datafile
    """
    conn = sqlite3.connect(database_path)
    conn.isolation_level = "EXCLUSIVE"
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    # some optimization to make it faster
    c.execute("PRAGMA page_size=627680;")
    c.execute("PRAGMA default_cache_size=320000;")
    c.execute("PRAGMA synchronous=OFF;")
    c.execute("PRAGMA count_changes=OFF;")
    c.execute("PRAGMA temp_store=MEMORY;")
    c.execute("PRAGMA journal_mode=MEMORY;")

    input = codecs.open(sentences_path, "r", encoding="utf-8")

    wrong_flags = {}
    if tags_path:
        wrong_flags = get_sentences_with_tag(tags_path, "@change flag")

    user_lang_nbr_ngram = defaultdict(int)
    for size in range(UP_TO_N_GRAM, 1, -1):
        hyper_lang_ngram = defaultdict(lambda: defaultdict(int))
        hyper_lang_nbr_ngram = defaultdict(int)

        line_number = 0
        input.seek(0)
        for line in input:
            if line_number % 10000 == 0:
                print_status_line(size, line_number)

            line_number += 1
            try:
                cols = line.rstrip().split("\t")
                sentence_id, lang, text, user = cols[:4]
            except IndexError:
                print(
                    "Skipped erroneous line {}: {}".format(line_number, line)
                )
                continue

            # we ignore the sentence with an unset language
            if lang == "\\N" or lang == "":
                continue

            # we ignore the sentence with wrong flag
            if sentence_id in wrong_flags:
                continue

            # update counts
            user_lang_nbr_ngram[(user, lang)] += len(text)
            nbr_ngram_line = len(text) - size
            if nbr_ngram_line > 0:
                hyper_lang_nbr_ngram[lang] += nbr_ngram_line
                for i in range(nbr_ngram_line + 1):
                    ngram = text[i : i + size]
                    hyper_lang_ngram[lang][ngram] += 1

        print_status_line(size, line_number)
        print(" done".format(line_number))

        print("Inserting ngrams of size {}...".format(size))

        table = TABLE_NGRAM + str(size)
        for lang, currentLangNgram in hyper_lang_ngram.items():
            for ngram, hit in currentLangNgram.items():
                freq = float(hit) / hyper_lang_nbr_ngram[lang]

                if lang in IDEOGRAM_LANGS:
                    if freq > IDEOGRAM_NGRAM_FREQ_LIMIT:
                        c.execute(
                            INSERT_NGRAM % (table), (ngram, lang, hit, freq)
                        )
                else:
                    if freq > NGRAM_FREQ_LIMIT:
                        c.execute(
                            INSERT_NGRAM % (table), (ngram, lang, hit, freq)
                        )
            conn.commit()

    print("Inserting user stats...")
    for (user, lang), hit in user_lang_nbr_ngram.items():
        if hit > USR_LANG_LIMIT:
            c.execute(INSERT_USR_STAT % (TABLE_USR_STAT), (user, lang, hit))
    conn.commit()
    c.close()


def create_indexes_db(database_path):
    """Add indexes on the database to make request faster

    Parameters
    ----------
    database_path : str
        the path of the Tatodetect sqlite database
    """
    conn = sqlite3.connect(database_path)
    conn.isolation_level = "EXCLUSIVE"
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("PRAGMA page_size=627680;")
    c.execute("PRAGMA default_cache_size=320000;")
    c.execute("PRAGMA synchronous=OFF;")
    c.execute("PRAGMA count_changes=OFF;")
    c.execute("PRAGMA temp_store=MEMORY;")
    c.execute("PRAGMA journal_mode=MEMORY;")

    for i in range(2, UP_TO_N_GRAM + 1):
        c.execute(
            """
            CREATE INDEX
                gram_grams%d_idx
            ON grams%d(gram);
            """
            % (i, i)
        )
    c.execute(
        """
        CREATE UNIQUE INDEX
            lang_user_users_langs_idx
        ON
            %s(lang,user);
        """
        % (TABLE_USR_STAT)
    )
    c.execute(
        """
        CREATE INDEX
           user_%s_idx
        ON %s(user);
        """
        % (TABLE_USR_STAT, TABLE_USR_STAT)
    )
    conn.commit()
    c.close()


def get_sentences_with_tag(tags_path, tag_name):
    """Fetch ids of all Tatoeba sentences tagged with this tag

    Parameters
    ----------
    tags_path : str
        the path of the Tatoeba tags.csv dump file
    tag_name : str
        the tag for which tagged sentences are looked for

    Returns
    -------
    dict
        the ids of the tagged sentences mapping to True
    """
    tagged = set()
    try:
        with open(tags_path, "r", encoding="utf-8") as f:
            for line in f:
                sentence_id, tag = line.rstrip().split("\t")
                if tag == tag_name:
                    tagged.add(sentence_id)
    except IndexError:
        pass

    return tagged


def print_status_line(size, line_number):
    """Print the progress of the ngram counting

    Parameters
    ----------
    size : int
        the size of the ngram (i.e. 3 for 3-grams)
    line_number : int
        the index of the processed line in sentences csv file
    """
    msg = (
        f"\rGenerating ngrams of size {size} "
        f"(reading CSV file... {line_number} lines)"
    )
    print(msg, end="")
    sys.stdout.flush()


if __name__ == "__main__":

    if len(sys.argv) < 3:
        fnames = "sentences_detailed.csv", "ngrams.db", "tags.csv"
        msg = f"Usage: {sys.argv[0]} <{fnames[0]}> <{fnames[1]}> [{fnames[2]}]"
        print(msg)
        sys.exit(1)

    sentences_path = sys.argv[1]
    database_path = sys.argv[2]
    try:
        tags_path = sys.argv[3]
    except IndexError:
        tags_path = None

    # we first delete the old database
    if os.path.isfile(database_path):
        os.remove(database_path)

    print("Start generating database...")
    generate_db(database_path)

    print("generating n-grams...")
    generate_n_grams(database_path, sentences_path, tags_path)

    print("creating indexes...")
    create_indexes_db(database_path)
