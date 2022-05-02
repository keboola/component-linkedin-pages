from keboola.http_client import HttpClient

BASE_URL = "https://api.linkedin.com"
API_VERSION = "v2"

ENDPOINT_ORG_PAGE_STATS = "organizationPageStatistics"


class LinkedInClientException(Exception):
    pass


class LinkedInClient(HttpClient):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        base_url = "/".join([BASE_URL, API_VERSION])
        super().__init__(base_url)

    def get_organization_page_statistics(self, organization: str):
        params = {"oauth2_access_token": self.access_token,
                  "q": "organization",
                  "organization": organization}
        return self.get(endpoint_path=ENDPOINT_ORG_PAGE_STATS, params=params)
