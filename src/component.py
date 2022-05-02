import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from linkedin import LinkedInClient, LinkedInClientException

KEY_ORGANIZATION_ID = "organisation_id"

REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self) -> None:
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        access_token = self.get_access_token()
        client = LinkedInClient(access_token)
        organisation_id = params.get(KEY_ORGANIZATION_ID)
        urn = f"urn:li:organization:{organisation_id}"
        try:
            data = client.get_organization_page_statistics(urn)
        except LinkedInClientException as client_exc:
            raise UserException(client_exc) from client_exc
        print(data)

    def get_access_token(self) -> str:
        if "access_token" not in self.configuration.oauth_credentials["data"]:
            raise UserException("Access token not available. Retry Authorization process")
        return self.configuration.oauth_credentials["data"]["access_token"]


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
