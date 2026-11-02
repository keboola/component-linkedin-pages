import unittest
from urllib.parse import quote

from linkedin.client import ENDPOINT_SOCIAL_ACTIONS, LinkedInClient


class TestClientUrls(unittest.TestCase):

    def test_post_urn_in_path_is_encoded_once(self):
        client = LinkedInClient(access_token="token")
        url = client._build_url(f"{ENDPOINT_SOCIAL_ACTIONS}/{quote('urn:li:share:1')}/comments")
        self.assertTrue(url.endswith("/socialActions/urn%3Ali%3Ashare%3A1/comments"), url)


if __name__ == "__main__":
    unittest.main()
