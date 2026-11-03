import csv
import json
import os
import tempfile
import unittest

import mock
from freezegun import freeze_time

from component import LinkedInPagesExtractor, POST_STATISTICS_TABLE_COLUMNS
from linkedin import URN
from linkedin.client import POST_STATS_BATCH_SIZE, LinkedInClient

ORGANIZATION = "urn:li:organization:5"


def stats_element(post_urn: str, impressions: int, entity_key: str = "share"):
    return {entity_key: post_urn, "organizationalEntity": ORGANIZATION,
            "totalShareStatistics": {"impressionCount": impressions, "uniqueImpressionsCount": 7, "clickCount": 2,
                                     "likeCount": 3, "commentCount": 1, "shareCount": 0, "engagement": 0.05}}


class TestPostStatisticsClient(unittest.TestCase):

    def setUp(self):
        self.client = LinkedInClient(access_token="token")
        response = mock.Mock(**{"json.return_value": {"elements": [], "paging": {}}})
        self.client.get_raw = mock.Mock(return_value=response)

    def requested_urls(self):
        return [call.args[0] for call in self.client.get_raw.call_args_list]

    def test_shares_and_ugc_posts_are_queried_by_their_list_params(self):
        post_urns = [URN.from_str("urn:li:share:1"), URN.from_str("urn:li:ugcPost:2")]
        list(self.client.get_share_statistics_for_posts(ORGANIZATION, post_urns))

        self.assertEqual(self.requested_urls(), [
            "organizationalEntityShareStatistics?shares=List(urn%3Ali%3Ashare%3A1)",
            "organizationalEntityShareStatistics?ugcPosts=List(urn%3Ali%3AugcPost%3A2)"])
        params = self.client.get_raw.call_args.kwargs["params"]
        self.assertEqual(params, {"q": "organizationalEntity", "organizationalEntity": ORGANIZATION})

    def test_posts_are_requested_in_batches(self):
        post_urns = [URN("share", i) for i in range(POST_STATS_BATCH_SIZE + 1)]
        list(self.client.get_share_statistics_for_posts(ORGANIZATION, post_urns))

        urls = self.requested_urls()
        self.assertEqual(len(urls), 2)
        self.assertEqual(urls[0].count("urn%3Ali%3Ashare%3A"), POST_STATS_BATCH_SIZE)
        self.assertTrue(urls[1].endswith(f"List(urn%3Ali%3Ashare%3A{POST_STATS_BATCH_SIZE})"), urls[1])

    def test_other_entity_types_are_not_requested(self):
        list(self.client.get_share_statistics_for_posts(ORGANIZATION, [URN("video", 1)]))
        self.client.get_raw.assert_not_called()


class TestPostStatisticsTable(unittest.TestCase):

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
        self.component.client = mock.Mock()

    def read_output(self, table_name: str):
        csv_path = os.path.join(self.data_dir, "out", "tables", f"{table_name}.csv")
        with open(f"{csv_path}.manifest") as manifest_file:
            manifest = json.load(manifest_file)
        with open(csv_path) as csv_file:
            rows = list(csv.DictReader(csv_file, fieldnames=manifest["columns"]))
        return manifest, rows

    def run_posts(self, post_statistics: bool):
        self.component.post_statistics = post_statistics
        posts = [{"id": "urn:li:share:1", "author": ORGANIZATION}, {"id": "urn:li:ugcPost:2", "author": ORGANIZATION}]
        self.component.client.get_posts_by_author.side_effect = [iter(posts), iter([])]
        self.component.client.get_comments_on_post.return_value = []
        self.component.client.get_likes_on_post.return_value = []
        self.component.client.get_share_statistics_for_posts.return_value = iter(
            [stats_element("urn:li:share:1", 1642), {"ugcPost": "urn:li:ugcPost:2", "organizationalEntity": ORGANIZATION,
                                                     "totalShareStatistics": {"impressionCount": 10}}])
        tables = self.component.get_all_posts_based_tables([URN.from_str(ORGANIZATION)])
        for table in tables:
            self.component.save_table(table)
        return tables

    def test_disabled_by_default_does_not_call_the_api(self):
        tables = self.run_posts(post_statistics=False)

        self.assertNotIn("post_statistics", [table.name for table in tables])
        self.component.client.get_share_statistics_for_posts.assert_not_called()

    @freeze_time("2026-10-30 23:30:00")
    def test_enabled_writes_dated_snapshot_per_post(self):
        self.run_posts(post_statistics=True)

        self.component.client.get_share_statistics_for_posts.assert_called_once_with(
            ORGANIZATION, [URN.from_str("urn:li:share:1"), URN.from_str("urn:li:ugcPost:2")])
        manifest, rows = self.read_output("post_statistics")
        self.assertEqual(manifest["primary_key"], ["post_urn", "snapshot_date"])
        self.assertTrue(manifest["incremental"])
        self.assertEqual(manifest["columns"], POST_STATISTICS_TABLE_COLUMNS)
        self.assertEqual(rows[0]["post_urn"], "urn:li:share:1")
        self.assertEqual(rows[0]["snapshot_date"], "2026-10-30")
        self.assertEqual(rows[0]["impressionCount"], "1642")
        self.assertEqual(rows[0]["engagement"], "0.05")
        self.assertEqual(rows[1]["post_urn"], "urn:li:ugcPost:2")
        self.assertEqual(rows[1]["uniqueImpressionsCount"], "")

    def test_no_statistics_returned_does_not_output_the_table(self):
        self.component.post_statistics = True
        self.component.client.get_share_statistics_for_posts.return_value = iter([])
        table = self.component.get_post_statistics_table({URN.from_str("urn:li:share:1"): ORGANIZATION})
        self.component.save_table(table)

        self.assertFalse(table.is_saved)
        self.assertFalse(os.path.exists(os.path.join(self.data_dir, "out", "tables", "post_statistics.csv")))


if __name__ == "__main__":
    unittest.main()
