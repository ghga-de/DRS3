# Copyright 2021 - 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main business-logic of this service"""

import contextlib
import re
import uuid
from datetime import timedelta

from ghga_service_commons.utils import utc_dates
from hexkit.providers.s3 import S3Config
from pydantic import Field, PositiveInt, field_validator
from pydantic_settings import BaseSettings

from dcs.adapters.outbound.http import exceptions
from dcs.adapters.outbound.http.api_calls import (
    delete_secret_from_ekss,
    get_envelope_from_ekss,
)
from dcs.adapters.outbound.s3 import S3ObjectStorage
from dcs.core import models
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.outbound.dao import DrsObjectDaoPort, ResourceNotFoundError
from dcs.ports.outbound.event_pub import EventPublisherPort


class DataRepositoryConfig(BaseSettings):
    """Config parameters needed for the DataRepository."""

    outbox_bucket: str
    drs_server_uri: str = Field(
        ...,
        description="The base of the DRS URI to access DRS objects. Has to start with 'drs://'"
        + " and end with '/'.",
        examples=["drs://localhost:8080/"],
    )
    retry_access_after: int = Field(
        120,
        description="When trying to access a DRS object that is not yet in the outbox, instruct"
        + " to retry after this many seconds.",
    )
    ekss_base_url: str = Field(
        ...,
        description="URL containing host and port of the EKSS endpoint to retrieve"
        + " personalized envelope from",
        examples=["http://ekss:8080/"],
    )
    presigned_url_expires_after: PositiveInt = Field(
        ...,
        description="Expiration time in seconds for presigned URLS. Positive integer required",
        examples=[30],
    )
    cache_timeout: int = Field(
        7,
        description="Time in days since last access after which a file present in the "
        + "outbox should be unstaged and has to be requested from permanent storage again "
        + "for the next request.",
    )

    @field_validator("drs_server_uri")
    @classmethod
    def check_server_uri(cls, value: str):
        """Checks the drs_server_uri."""
        if not re.match(r"^drs://.+/$", value):
            raise ValueError(
                f"The drs_server_uri has to start with 'drs://' and end with '/', got : {value}"
            )

        return value


class ObjectStorageNodeConfig(BaseSettings):
    """Configuration for one specific object storage node"""

    bucket: str
    credentials: S3Config


class ObjectStorageConfig(BaseSettings):
    """Configuration for all available object storage nodes"""

    object_storages: dict[str, ObjectStorageNodeConfig]


class ObjectStorages:
    """Constructor to instantiate multiple object storage objects from config"""

    def __init__(self, *, config: ObjectStorageConfig) -> None:
        self._config = config
        self.object_storages: dict[str, S3ObjectStorage] = {}

    def __getitem__(self, key):
        """It's some kind of magic"""
        if not self.object_storages:
            self._create_object_storages()
        return self.object_storages[key]

    def _create_object_storages(self):
        """Create object storage instances from config"""
        for node_label, node_config in self._config.object_storages.items():
            self.object_storages[node_label] = S3ObjectStorage(
                config=node_config.credentials
            )


class DataRepository(DataRepositoryPort):
    """A service that manages a registry of DRS objects."""

    def __init__(
        self,
        *,
        config: DataRepositoryConfig,
        drs_object_dao: DrsObjectDaoPort,
        object_storages: ObjectStorages,
        event_publisher: EventPublisherPort,
    ):
        """Initialize with essential config params and outbound adapters."""
        self._config = config
        self._event_publisher = event_publisher
        self._drs_object_dao = drs_object_dao
        self._object_storages = object_storages

    def _get_drs_uri(self, *, drs_id: str) -> str:
        """Construct DRS URI for the given DRS ID."""
        return f"{self._config.drs_server_uri}{drs_id}"

    def _get_model_with_self_uri(
        self, *, drs_object: models.DrsObject
    ) -> models.DrsObjectWithUri:
        """Add the DRS self URI to an DRS object."""
        return models.DrsObjectWithUri(
            **drs_object.dict(),
            self_uri=self._get_drs_uri(drs_id=drs_object.file_id),
        )

    async def _get_access_model(
        self, *, drs_object: models.DrsObject, s3_endpoint_alias: str
    ) -> models.DrsObjectWithAccess:
        """Get a DRS Object model with access information."""
        object_storage = self._object_storages[s3_endpoint_alias]
        access_url = await object_storage.get_object_download_url(
            bucket_id=self._config.outbox_bucket,
            object_id=drs_object.object_id,
            expires_after=self._config.presigned_url_expires_after,
        )

        return models.DrsObjectWithAccess(
            **drs_object.dict(),
            self_uri=self._get_drs_uri(drs_id=drs_object.file_id),
            access_url=access_url,
        )

    async def access_drs_object(self, *, drs_id: str) -> models.DrsObjectResponseModel:
        """
        Serve the specified DRS object with access information.
        If it does not exists in the outbox, yet, a RetryAccessLaterError is raised that
        instructs to retry the call after a specified amount of time.
        """
        # make sure that metadata for the DRS object exists in the database:
        try:
            drs_object_with_access_time = await self._drs_object_dao.get_by_id(drs_id)
        except ResourceNotFoundError as error:
            raise self.DrsObjectNotFoundError(drs_id=drs_id) from error

        drs_object = models.DrsObject(
            **drs_object_with_access_time.dict(exclude={"last_accessed"})
        )

        drs_object_with_uri = self._get_model_with_self_uri(drs_object=drs_object)

        s3_endpoint_alias = drs_object.s3_endpoint_alias
        object_storage = self._object_storages[s3_endpoint_alias]

        # check if the file corresponding to the DRS object is already in the outbox:
        if not await object_storage.does_object_exist(
            bucket_id=self._config.outbox_bucket, object_id=drs_object.object_id
        ):
            # publish an event to request a stage of the corresponding file:
            await self._event_publisher.unstaged_download_requested(
                drs_object=drs_object_with_uri,
                s3_endpoint_alias=s3_endpoint_alias,
                target_bucket_id=self._config.outbox_bucket,
            )

            # instruct to retry later:
            raise self.RetryAccessLaterError(
                retry_after=self._config.retry_access_after
            )

        # Successfully staged, update access information now
        drs_object_with_access_time.last_accessed = utc_dates.now_as_utc()
        try:
            await self._drs_object_dao.update(drs_object_with_access_time)
        except ResourceNotFoundError as error:
            raise self.DrsObjectNotFoundError(drs_id=drs_id) from error

        drs_object_with_access = await self._get_access_model(
            drs_object=drs_object, s3_endpoint_alias=s3_endpoint_alias
        )

        # publish an event indicating the served download:
        await self._event_publisher.download_served(
            drs_object=drs_object_with_uri,
            s3_endpoint_alias=s3_endpoint_alias,
            target_bucket_id=self._config.outbox_bucket,
        )

        # CLI needs to have the encrypted size to correctly download all file parts
        encrypted_size = await object_storage.get_object_size(
            bucket_id=self._config.outbox_bucket, object_id=drs_object.object_id
        )
        return drs_object_with_access.convert_to_drs_response_model(size=encrypted_size)

    async def cleanup_outbox(self, *, s3_endpoint_alias: str):
        """
        Check if files present in the outbox have outlived their allocated time and remove
        all that do.
        For each file in the outbox, its 'last_accessed' field is checked and compared
        to the current datetime. If the threshold configured in the cache_timeout option
        is met or exceeded, the corresponding file is removed from the outbox.
        """
        threshold = utc_dates.now_as_utc() - timedelta(days=self._config.cache_timeout)

        object_storage = self._object_storages[s3_endpoint_alias]
        # filter to get all files in outbox that should be removed
        object_ids = await object_storage.list_all_object_ids(
            bucket_id=self._config.outbox_bucket
        )
        for object_id in object_ids:
            try:
                drs_object = await self._drs_object_dao.find_one(
                    mapping={"object_id": object_id}
                )
            except ResourceNotFoundError as error:
                raise self.CleanupError(
                    object_id=object_id, from_error=error
                ) from error

            # only remove file if last access is later than cache timeout days ago
            if drs_object.last_accessed <= threshold:
                try:
                    await object_storage.delete_object(
                        bucket_id=self._config.outbox_bucket, object_id=object_id
                    )
                except (
                    object_storage.ObjectError,
                    object_storage.ObjectStorageProtocolError,
                ) as error:
                    raise self.CleanupError(
                        object_id=object_id, from_error=error
                    ) from error

    async def register_new_file(
        self, *, file: models.DrsObjectBase, s3_endpoint_alias: str
    ):
        """Register a file as a new DRS Object."""
        object_id = str(uuid.uuid4())
        drs_object = models.DrsObject(
            **file.model_dump(),
            object_id=object_id,
            s3_endpoint_alias=s3_endpoint_alias,
        )

        file_with_access_time = models.AccessTimeDrsObject(
            **drs_object.model_dump(),
            last_accessed=utc_dates.now_as_utc(),
        )
        # write file entry to database
        await self._drs_object_dao.insert(file_with_access_time)

        # publish message that the drs file has been registered
        drs_object_with_uri = self._get_model_with_self_uri(drs_object=drs_object)
        await self._event_publisher.file_registered(drs_object=drs_object_with_uri)

    async def serve_envelope(self, *, drs_id: str, public_key: str) -> str:
        """
        Retrieve envelope for the object with the given DRS ID

        :returns: base64 encoded envelope bytes
        """
        try:
            drs_object = await self._drs_object_dao.get_by_id(id_=drs_id)
        except ResourceNotFoundError as error:
            raise self.DrsObjectNotFoundError(drs_id=drs_id) from error

        try:
            envelope = get_envelope_from_ekss(
                secret_id=drs_object.decryption_secret_id,
                receiver_public_key=public_key,
                api_base=self._config.ekss_base_url,
            )
        except (
            exceptions.BadResponseCodeError,
            exceptions.RequestFailedError,
        ) as error:
            raise self.APICommunicationError(
                api_url=self._config.ekss_base_url
            ) from error
        except exceptions.SecretNotFoundError as error:
            raise self.EnvelopeNotFoundError(object_id=drs_object.object_id) from error

        return envelope

    async def delete_file(self, *, file_id: str) -> None:
        """Deletes a file from the outbox storage, the internal database and the
        corresponding secret from the secrets store.
        If no file or secret with that id exists, do nothing.

        Args:
            file_id: id for the file to delete.
        """
        # Get drs object from db
        try:
            drs_object = await self._drs_object_dao.get_by_id(id_=file_id)
        except ResourceNotFoundError:
            # If the db entry does not exist, we are done, as it is deleted last
            return

        # call EKSS to remove file secret from vault
        with contextlib.suppress(exceptions.SecretNotFoundError):
            delete_secret_from_ekss(
                secret_id=drs_object.decryption_secret_id,
                api_base=self._config.ekss_base_url,
            )

        object_storage = self._object_storages[drs_object.s3_endpoint_alias]

        # Try to remove file from S3
        with contextlib.suppress(object_storage.ObjectNotFoundError):
            await object_storage.delete_object(
                bucket_id=self._config.outbox_bucket, object_id=drs_object.object_id
            )

        # Remove file from database and send success event
        # Should not fail as we got the DRS object by the same ID
        await self._drs_object_dao.delete(id_=file_id)
        await self._event_publisher.file_deleted(file_id=file_id)
