import asyncio

from src.config.constants import EmailServiceType
from src.web.routes import email as email_routes


def test_cloudmail_alias_is_accepted():
    assert EmailServiceType("cloud_mail") == EmailServiceType.CLOUDMAIL


def test_email_service_types_include_cloud_mail():
    result = asyncio.run(email_routes.get_service_types())
    cloudmail_type = next(item for item in result["types"] if item["value"] == "cloudmail")

    assert cloudmail_type["label"] == "CloudMail"
    field_names = [field["name"] for field in cloudmail_type["config_fields"]]
    assert "base_url" in field_names
    assert "admin_password" in field_names
    assert "domain" in field_names
