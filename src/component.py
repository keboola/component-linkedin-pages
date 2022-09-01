import logging

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from linkedin import LinkedInClient, LinkedInClientException

KEY_ORGANIZATION_ID = "organization_id"

REQUIRED_PARAMETERS = [KEY_ORGANIZATION_ID]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self) -> None:
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params: dict = self.configuration.parameters

        access_token = self.get_access_token()
        client = LinkedInClient(access_token)
        organisation_id = params.get(KEY_ORGANIZATION_ID)
        try:
            data1 = client.get_organization_page_statistics(organisation_id)
            print(data1)
        except LinkedInClientException as client_exc:
            raise UserException(client_exc) from client_exc
        try:
            data2 = client.get_organization_acls()
            print(data2)
        except LinkedInClientException as client_exc:
            raise UserException(client_exc) from client_exc

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
