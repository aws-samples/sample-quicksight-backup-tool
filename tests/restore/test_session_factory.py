from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from quicksight_restore.models.config import AuthConfig
from quicksight_restore.models.errors import RestoreConfigurationError
from quicksight_restore.session_factory import SessionFactory


ROLE_ARN = "arn:aws:iam::111111111111:role/RestoreSource"


class FakeCoreSession:
    def __init__(self):
        self.client_calls = []

    def create_client(self, service_name, **kwargs):
        self.client_calls.append((service_name, kwargs))
        return object()


class FakeBotoSession:
    created = []
    no_source_credentials = False

    def __init__(self, profile_name=None, region_name=None, botocore_session=None, **kwargs):
        self.profile_name = profile_name
        self.region_name = region_name
        self._session = botocore_session or FakeCoreSession()
        self.is_assumed = botocore_session is not None
        self.client_calls = []
        self.__class__.created.append(self)

    def get_credentials(self):
        if self.is_assumed:
            return self._session.get_credentials()
        if self.no_source_credentials:
            return None
        return object()

    def client(self, service_name, region_name=None):
        value = {
            "session": self,
            "service_name": service_name,
            "region_name": region_name,
        }
        self.client_calls.append(value)
        return value


class FakeFetcher:
    instances = []
    failure = None

    def __init__(self, client_creator, source_credentials, role_arn, extra_args=None, **kwargs):
        self.client_creator = client_creator
        self.source_credentials = source_credentials
        self.role_arn = role_arn
        self.extra_args = dict(extra_args or {})
        self.fetch_count = 0
        self.__class__.instances.append(self)

    def fetch_credentials(self):
        self.fetch_count += 1
        if self.failure is not None:
            raise self.failure
        return {
            "access_key": "ASIA{0:04d}".format(self.fetch_count),
            "secret_key": "secret-{0}".format(self.fetch_count),
            "token": "token-{0}".format(self.fetch_count),
            "expiry_time": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        }


@pytest.fixture(autouse=True)
def fake_session_dependencies(monkeypatch):
    FakeBotoSession.created = []
    FakeBotoSession.no_source_credentials = False
    FakeFetcher.instances = []
    FakeFetcher.failure = None
    monkeypatch.setattr("quicksight_restore.session_factory.boto3.Session", FakeBotoSession)
    monkeypatch.setattr(
        "quicksight_restore.session_factory.AssumeRoleCredentialFetcher",
        FakeFetcher,
    )


def role_auth(external_id="reviewed-external-id", sts_region="us-west-2"):
    return AuthConfig(
        profile="source-profile",
        role_arn=ROLE_ARN,
        external_id=external_id,
        role_session_name="restore-session",
        sts_region=sts_region,
    )


def test_assume_role_credentials_refresh_after_initial_fetch():
    auth = role_auth()
    factory = SessionFactory(auth, AuthConfig(profile="target"))

    session = factory._session("source", "us-east-1", auth)
    fetcher = FakeFetcher.instances[0]

    assert fetcher.fetch_count == 1
    assert fetcher.role_arn == ROLE_ARN
    assert fetcher.extra_args == {
        "RoleSessionName": "restore-session",
        "ExternalId": "reviewed-external-id",
    }
    source_session = next(item for item in FakeBotoSession.created if not item.is_assumed)
    assert source_session.region_name == "us-west-2"
    assert session._session.get_config_variable("region") == "us-east-1"

    credentials = session._session.get_credentials()
    credentials._expiry_time = datetime.now(timezone.utc) - timedelta(seconds=1)
    refreshed = credentials.get_frozen_credentials()

    assert fetcher.fetch_count == 2
    assert refreshed.access_key == "ASIA0002"


def test_source_and_target_refresh_state_is_isolated_and_sessions_are_cached():
    auth = role_auth()
    factory = SessionFactory(auth, auth)

    first_source = factory._session("source", "us-east-1", auth)
    second_source = factory._session("source", "us-east-1", auth)
    target = factory._session("target", "us-east-1", auth)

    assert first_source is second_source
    assert target is not first_source
    assert len(FakeFetcher.instances) == 2
    assert all(fetcher.fetch_count == 1 for fetcher in FakeFetcher.instances)


def test_sts_region_and_external_id_participate_in_session_cache_identity():
    auth = role_auth()
    factory = SessionFactory(auth, AuthConfig(profile="target"))

    first = factory._session("source", "us-east-1", auth)
    auth.external_id = "different-external-id"
    second = factory._session("source", "us-east-1", auth)
    auth.sts_region = "eu-west-1"
    third = factory._session("source", "us-east-1", auth)

    assert first is not second
    assert second is not third
    assert len(FakeFetcher.instances) == 3
    cache_text = repr(list(factory._sessions))
    assert "reviewed-external-id" not in cache_text
    assert "different-external-id" not in cache_text


def test_missing_source_credentials_and_initial_assume_failure_are_wrapped():
    auth = role_auth()
    factory = SessionFactory(auth, AuthConfig(profile="target"))
    FakeBotoSession.no_source_credentials = True

    with pytest.raises(RestoreConfigurationError, match="Unable to establish source"):
        factory._session("source", "us-east-1", auth)

    FakeBotoSession.no_source_credentials = False
    FakeFetcher.failure = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "AssumeRole",
    )
    with pytest.raises(RestoreConfigurationError, match="Unable to establish target"):
        factory._session("target", "us-east-1", auth)
