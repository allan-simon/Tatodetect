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
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# languages that don't use an alphabet
# we put them apart as they are likely to have a lot of different
# ngrams and by so need to have a lower limit for the ngrams we kept
# for that languages
IDEOGRAM_LANGS = ("wuu", "yue", "cmn")
IDEOGRAM_NGRAM_FREQ_LIMIT = 0.000005
NGRAM_FREQ_LIMIT = 0.00001
# number of 1-gram a user must have submitted in one language to
# be considered as possibly contributing in that languages
# note that this number is currently purely arbitrary
MIN_USER_CONTRIB_IN_LANG = 100
# we will generate the ngram from 2-gram to X-grams
MIN_NGRAM_SIZE = 2
MAX_NGRAM_SIZE = 5
# some names of the table in the database
TABLE_NGRAM = "grams"
TABLE_STAT = "langstat"
TABLE_USR_STAT = "users_langs"


class NgramCounterDB:
    """A sqlite database for tracking:

    The occurrence counts of every n-gram found in the
    Tatoeba corpus

    Statistics related to user contributions to Tatoeba
    in various languages
    """

    def __init__(self, db_path: Path) -> None:
        """
        Parameters
        ----------
        db_path : Path
            the location of the sqlite database
        """
        self._fp = db_path

        self._conn = self._init_db()

    def count_ngram_hits(
        self,
        sentences_detailed_path: Path,
        sentence_blacklist: list,
        buffer_length: int = 5000000,
    ) -> None:
        """Count n-grams' occurrences for each Tatoeba language

        A contribution score equal to sum(len(sentences))
        is also computed for every language a user contributed to.

        All results are gradually saved into database tables in
        order to mitigate the memory footprint of the process.

        Parameters
        ----------
        sentences_detailed_path : Path
            path of the 'sentences_detailed.csv' weekly dump file
        sentence_blacklist : list
            the integer ids of the sentences that are not taken
            into account for this counting
        buffer_length : int, optional
            the maximum number of n-grams for which counts are kept in RAM,
            by default 5000000
            Note thet process speed mainly depends on the amount of data
            written to disk and consequently increases with the buffer size.
        """
        # delete older database found at this path
        if self._fp.exists():
            print(f"Warning, the older n-gram counter data will be overwritten")
            self._fp.unlink()
            self._init_db()

        user_lang_score = defaultdict(int)
        for n in range(MAX_NGRAM_SIZE, 1, -1):
            table_name = f"{TABLE_NGRAM}{n}"
            lang_ngram_cnt = defaultdict(lambda: defaultdict(int))
            with open(sentences_detailed_path, "r", encoding="utf-8") as f:
                for line_id, line in enumerate(f):
                    self._print_status(line_id, gram_length=n)
                    try:
                        fields = line.rstrip().split("\t")
                        sent_id, lang, text, user = fields[:4]
                    except IndexError:
                        print(f"Skipped erroneous line {line_id}: {line}")
                        continue

                    # we ignore the sentence with an unset language
                    if lang == "\\N" or lang == "":
                        continue

                    # we ignore the sentence with wrong flag
                    if int(sent_id) in sentence_blacklist:
                        continue

                    # update user contribution score during last reader loop
                    if n == MIN_NGRAM_SIZE:
                        user_lang_score[(user, lang)] += len(text)

                    # increment hit counts for each ngram in the sentence
                    for i in range(len(text) - n + 1):
                        ngram = text[i : i + n]
                        lang_ngram_cnt[lang][ngram] += 1

                    # if buffer is full save it into table and then empty it
                    tot_ngrams = sum(len(v) for v in lang_ngram_cnt.values())
                    if tot_ngrams >= buffer_length:
                        for lang, ngram_hits in lang_ngram_cnt.items():
                            self._upsert_hits(ngram_hits, lang, table_name)
                        lang_ngram_cnt = defaultdict(lambda: defaultdict(int))

            # move ngram hits from memory to table
            for lang, ngram_hits in lang_ngram_cnt.items():
                self._upsert_hits(ngram_hits, lang, table_name)

            self._print_status(line_id, gram_length=n, force=True)
            print(" done")

    def _init_db(self):

        conn = sqlite3.connect(self._fp)
        with conn:
            # some optimization to make connection faster
            conn.execute("PRAGMA journal_mode=MEMORY;")
            conn.execute("PRAGMA temp_store=MEMORY;")

        with conn:
            # create a table for each n-gram type counts
            for n in range(MIN_NGRAM_SIZE, MAX_NGRAM_SIZE + 1):
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS %s (
                    'gram' text not null,
                    'lang'text not null,
                    'hit'  int not null,
                    PRIMARY KEY("gram","lang")
                    );
                    """
                    % (TABLE_NGRAM + str(n))
                )
            # create a table for storing the sentences counts of users
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    'user' text not null,
                    'lang' text not null ,
                    'total' int not null default 0
                );
                """
                % (TABLE_USR_STAT)
            )

        return conn

    def _upsert_hits(self, ngram_hits, lang, table_name):
        """Update n-gram hit count values in this table"""
        with self._conn:
            for ngram, hits in ngram_hits.items():
                ngram = ngram.replace("'", "''")
                sql = (
                    f"INSERT INTO {table_name} VALUES ('{ngram}', '{lang}', {hits}) "
                    f"ON CONFLICT (gram, lang) DO UPDATE SET hit = hit + {hits};"
                )
                self._conn.execute(sql)
            self._conn.execute("PRAGMA shrink_memory;")

    def _insert_user_stats(self, user_lang_score):
        """Insert user language contribution scores into table"""
        print("Inserting user stats...")
        with self._conn:
            self._conn.execute("PRAGMA shrink_memory;")
            for (user, lang), hit in user_lang_score.items():
                self._conn.execute(
                    "INSERT INTO %s VALUES (?,?,?);" % (TABLE_USR_STAT),
                    (user, lang, hit),
                )
            self._conn.execute("PRAGMA shrink_memory;")

    @staticmethod
    def _print_status(line_number, gram_length, force=False):
        """Keep track of n-gram counting progress"""
        if line_number % 10000 == 0 or force:
            msg = (
                f"\rGenerating ngrams of size {gram_length} "
                f"(reading CSV file... {line_number} lines)"
            )
            print(msg, end="")
            sys.stdout.flush()

    @property
    def path(self):

        return self._fp


class TatodetectDB:
    def __init__(self, db_path: Path) -> None:
        """
        Parameters
        ----------
        db_path : Path
            the location of the sqlite databse
        """
        self._fp = db_path

        self._conn = self._init_db()

    def extract_top_from(self, ngram_counter_db: NgramCounterDB) -> None:
        """Copy most significant content
        from an n-gram counter sqlite database

        Parameters
        ----------
        ngram_counter_db : NgramCounterDB
            a database containing all Tatoeba n-grams counts
        """
        # delete older database found at this path
        if self._fp.exists():
            print(f"Warning: the previous top data will be overwritten")
            self._fp.unlink()
            self._conn = self._init_db()

        c = self._conn.cursor()

        # attach top database to enable data transfert
        c.execute(f"ATTACH DATABASE '{ngram_counter_db.path}' AS counter")

        for n in range(MIN_NGRAM_SIZE, MAX_NGRAM_SIZE + 1):
            print(f"Extracting top {n}-grams from counter database")
            c.execute(
                f"""
                INSERT INTO main.grams{n}
                SELECT
                  gram,
                  counter.grams{n}.lang,
                  hit,
                  CAST(hit AS FLOAT) / CAST(lang_ngram_tots.tot AS FLOAT) AS percent
                FROM counter.grams{n}
                INNER JOIN (
                SELECT
                  lang, 
                  SUM(hit) AS tot
                FROM counter.grams{n}
                GROUP BY lang) AS lang_ngram_tots
                ON counter.grams{n}.lang = lang_ngram_tots.lang
                WHERE percent > (
                CASE
                  WHEN counter.grams{n}.lang IN {IDEOGRAM_LANGS}
                  THEN {IDEOGRAM_NGRAM_FREQ_LIMIT} ELSE {NGRAM_FREQ_LIMIT}
                END)
                """
            )

        print(f"Extracting top contributors")
        c.execute(
            f"""
            INSERT INTO main.users_langs
            SELECT * FROM counter.users_langs
            WHERE total > {MIN_USER_CONTRIB_IN_LANG}
            """
        )

        self._conn.commit()

        c.execute("DETACH DATABASE counter")

        c.close()

    def _init_db(self):

        conn = sqlite3.connect(self._fp)
        with conn:
            # create a table for each n-gram type counts
            for n in range(MIN_NGRAM_SIZE, MAX_NGRAM_SIZE + 1):
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS %s (
                    'gram' text not null,
                    'lang'text not null,
                    'hit'  int not null,
                    'percent' float not null default 0,
                    PRIMARY KEY("gram","lang")
                    );
                    """
                    % (TABLE_NGRAM + str(n))
                )
            # create a table for storing the sentences counts of users
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS %s (
                    'user' text not null,
                    'lang' text not null ,
                    'total' int not null default 0
                );
                """
                % (TABLE_USR_STAT)
            )

        return conn


def get_sentences_with_tag(tags_path: Path, tag_name: str) -> set:
    """Get the ids of the Tatoeba sentences with this tag

    Parameters
    ----------
    tags_path : Path
        the path of the Tatoeba tags.csv dump file
    tag_name : str
        the tag for which tagged sentences are looked for

    Returns
    -------
    set
        the integer ids of the tagged sentences mapping to True
    """
    tagged = set()
    try:
        with open(tags_path, "r", encoding="utf-8") as f:
            for line in f:
                sentence_id, tag = line.rstrip().split("\t")
                if tag == tag_name:
                    tagged.add(int(sentence_id))
    except IndexError:
        pass

    return tagged


if __name__ == "__main__":

    if len(sys.argv) < 3:
        fnames = "sentences_detailed.csv", "ngrams.db", "tags.csv"
        msg = f"Usage: {sys.argv[0]} <{fnames[0]}> <{fnames[1]}> [{fnames[2]}]"
        print(msg)
        sys.exit(1)

    sentences_detailed_path = Path(sys.argv[1])
    database_path = Path(sys.argv[2])
    try:
        tags_path = Path(sys.argv[3])
    except IndexError:
        tags_path = None

    sentence_blacklist = get_sentences_with_tag(tags_path, "@change flag")

    ngram_counter_path = database_path.parent.joinpath("ngram_counter.db")
    ngram_counter_db = NgramCounterDB(ngram_counter_path)
    ngram_counter_db.count_ngram_hits(
        sentences_detailed_path, sentence_blacklist, buffer_length=5000000
    )
    tatodetect_db = TatodetectDB(database_path)
    tatodetect_db.extract_top_from(ngram_counter_db)
