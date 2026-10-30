import csv
import json
import os
import tempfile
import unittest

import mock

from component import KEY_OUTPUT_TABLE_COLUMNS, LinkedInPagesExtractor, POSTS_TABLE_KNOWN_COLUMNS
from data_processing import create_table


class TestStableOutputColumns(unittest.TestCase):

    def setUp(self):
        self.data_dir = tempfile.mkdtemp()
        with open(os.path.join(self.data_dir, "config.json"), "w") as config_file:
            json.dump({"parameters": {"endpoints": "posts", "destination": {"load_type": "incremental_load"}}},
                      config_file)
        with mock.patch.dict(os.environ, {"KBC_DATADIR": self.data_dir}):
            self.component = LinkedInPagesExtractor()
        self.component.incremental = True
        self.component.debug = False
        self.component.tmp_state = {}

    def read_output(self, table_name: str):
        csv_path = os.path.join(self.data_dir, "out", "tables", f"{table_name}.csv")
        with open(f"{csv_path}.manifest") as manifest_file:
            columns = json.load(manifest_file)["columns"]
        with open(csv_path) as csv_file:
            rows = list(csv.DictReader(csv_file, fieldnames=columns))
        return columns, rows

    def test_posts_without_poll_keep_poll_columns(self):
        posts = [{"id": "urn:li:share:1", "commentary": "Plain post", "distribution": {"feedDistribution": "MAIN"}}]
        self.component.save_table(create_table(records=posts, table_name="posts", primary_key=["id"]))

        columns, rows = self.read_output("posts")
        self.assertTrue(set(POSTS_TABLE_KNOWN_COLUMNS).issubset(columns))
        self.assertEqual(len(columns), len(set(columns)))
        self.assertEqual(rows[0]["commentary"], "Plain post")
        self.assertEqual(rows[0]["distribution_feedDistribution"], "MAIN")
        self.assertEqual(rows[0]["content_poll_question"], "")

    def test_columns_from_previous_runs_are_kept(self):
        self.component.tmp_state = {KEY_OUTPUT_TABLE_COLUMNS: {"posts": ["id", "content_newType_field"],
                                                               "likes": ["URN", "post_urn", "actor", "agent"]}}
        posts = [{"id": "urn:li:share:1", "unseenField": "x"}]
        likes = [{"URN": "urn:li:like:1", "post_urn": "urn:li:share:1", "actor": "urn:li:person:1"}]
        self.component.save_table(create_table(records=posts, table_name="posts", primary_key=["id"]))
        self.component.save_table(create_table(records=likes, table_name="likes", primary_key=["URN"]))

        posts_columns, _ = self.read_output("posts")
        likes_columns, likes_rows = self.read_output("likes")
        self.assertIn("content_newType_field", posts_columns)
        self.assertIn("unseenField", posts_columns)
        self.assertEqual(sorted(likes_columns), ["URN", "actor", "agent", "post_urn"])
        self.assertEqual(likes_rows[0]["agent"], "")
        stored_columns = self.component.tmp_state[KEY_OUTPUT_TABLE_COLUMNS]
        self.assertEqual(stored_columns["posts"], posts_columns)
        self.assertEqual(stored_columns["likes"], likes_columns)

    def test_empty_table_is_not_saved_nor_stored(self):
        self.component.save_table(create_table(records=[], table_name="comments", primary_key=["id"]))

        self.assertFalse(os.path.exists(os.path.join(self.data_dir, "out", "tables", "comments.csv")))
        self.assertNotIn(KEY_OUTPUT_TABLE_COLUMNS, self.component.tmp_state)


if __name__ == "__main__":
    unittest.main()
