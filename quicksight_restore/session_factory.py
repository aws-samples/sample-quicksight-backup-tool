"""Shared cached boto3 sessions and explicitly-Regioned clients for restore."""

from typing import Any, Dict, Tuple
import hashlib
import threading

import boto3
from botocore.credentials import AssumeRoleCredentialFetcher, RefreshableCredentials
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from botocore.session import get_session

from .models.config import AuthConfig
from .models.errors import RestoreConfigurationError


class SessionFactory:
    """Create source/target clients through default, profile, or AssumeRole auth.

    Role sessions use botocore refreshable credentials so a restore can safely run
    beyond the initial STS credential lifetime. Source and target credential state is
    isolated by scope and no credential material is returned or serialized.
    """

    def __init__(self, source_auth: AuthConfig, target_auth: AuthConfig):
        self.source_auth = source_auth
        self.target_auth = target_auth
        self._sessions: Dict[Tuple[str, ...], boto3.Session] = {}
        self._clients: Dict[Tuple[str, str, str], Any] = {}
        self._lock = threading.RLock()

    def source_client(self, service_name: str, region_name: str) -> Any:
        return self._client("source", service_name, region_name, self.source_auth)

    def target_client(self, service_name: str, region_name: str) -> Any:
        return self._client("target", service_name, region_name, self.target_auth)

    def _client(self, scope: str, service_name: str, region_name: str, auth: AuthConfig) -> Any:
        if not region_name:
            raise RestoreConfigurationError("An explicit client Region is required")
        key = (scope, service_name, region_name)
        with self._lock:
            if key not in self._clients:
                session = self._session(scope, region_name, auth)
                self._clients[key] = session.client(service_name, region_name=region_name)
            return self._clients[key]

    def _session(self, scope: str, region_name: str, auth: AuthConfig) -> boto3.Session:
        sts_region = auth.sts_region or region_name
        external_id_fingerprint = (
            hashlib.sha256(auth.external_id.encode("utf-8")).hexdigest() if auth.external_id else ""
        )
        key = (
            scope,
            region_name,
            auth.profile or "",
            auth.role_arn or "",
            auth.role_session_name,
            sts_region,
            external_id_fingerprint,
        )
        with self._lock:
            if key in self._sessions:
                return self._sessions[key]
            try:
                if not auth.role_arn:
                    session = boto3.Session(profile_name=auth.profile, region_name=region_name)
                    self._sessions[key] = session
                    return session

                source_session = boto3.Session(profile_name=auth.profile, region_name=sts_region)
                source_credentials = source_session.get_credentials()
                if source_credentials is None:
                    raise NoCredentialsError()
                extra_args: Dict[str, str] = {
                    "RoleSessionName": auth.role_session_name,
                }
                if auth.external_id:
                    extra_args["ExternalId"] = auth.external_id
                fetcher = AssumeRoleCredentialFetcher(
                    client_creator=source_session._session.create_client,
                    source_credentials=source_credentials,
                    role_arn=auth.role_arn,
                    extra_args=extra_args,
                )
                metadata = fetcher.fetch_credentials()
                refreshable = RefreshableCredentials.create_from_metadata(
                    metadata=metadata,
                    refresh_using=fetcher.fetch_credentials,
                    method="assume-role",
                )
                botocore_session = get_session()
                botocore_session._credentials = refreshable
                botocore_session.set_config_variable("region", region_name)
                session = boto3.Session(botocore_session=botocore_session)
                self._sessions[key] = session
                return session
            except (
                BotoCoreError,
                ClientError,
                NoCredentialsError,
                KeyError,
                TypeError,
                ValueError,
            ) as error:
                raise RestoreConfigurationError(
                    "Unable to establish {0} AWS session: {1}".format(scope, error)
                )
