# Copyright 2021 - 2022 Universität Tübingen, DKFZ and EMBL
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

from attr import field
from pydantic import BaseSettings

from dcs.ports.outbound.event_broadcast import DrsEventBroadcasterPort
from dcs.ports.outbound.dao import (
    DrsObjectDaoPort,
    ResourceNotFoundError,
    ResourceAlreadyExistsError,
)
from dcs.ports.outbound.storage import ObjectStoragePort

from dcs.core import models


class DrsObjectNotFoundError(RuntimeError):
    """Raised when no DRS file was found with the specified DRS ID."""

    def __init__(self, *, drs_id: str):
        message = f"No DRS object with the following ID exists: {drs_id}"
        super().__init__(message)


class NoCorrespondingDrsObjectError(RuntimeError):
    """Raised when trying to refer to a DRS object using an external file ID that is not
    known."""

    def __init__(self, *, file_id: str):
        message = f"No DRS object matching the following file ID exists: {file_id}"
        super().__init__(message)


class ContentMissmatchError(RuntimeError):
    """Raised when referring to a DRS object with the right primary ID but not the
    right content ID."""

    def __init__(
        self, *, drs_id: str, observed_content_id: str, expected_content_id: str
    ):
        message = (
            f"Missmatch of the content for DRS Object with the DRS ID {drs_id}:"
            + f" got a content ID of {observed_content_id} but expected"
            + f" {expected_content_id}"
        )
        super().__init__(message)


class DrsObjectNotInOutbox(RuntimeError):
    """Raised when a DRS object was unexpectedly not in the outbox."""

    def __init__(self, *, drs_id: str):
        message = (
            f"Could not find the DRS Object with the following ID in the outbox: "
            + str(drs_id)
        )
        super().__init__(message)


class DrsObjectRegistryConfig(BaseSettings):
    """Config parameters needed for the DrsObjectRegistry."""

    outbox_bucket: str


class DrsObjectRegistryService:
    """A service that manages a registry of DRS objects."""

    def __init__(
        self,
        *,
        config: DrsObjectRegistryConfig,
        drs_object_dao: DrsObjectDaoPort,
        object_storage: ObjectStoragePort,
        event_broadcaster: DrsEventBroadcasterPort,
    ):
        """Initialize with essential config params and outbound adapters."""

        self._outbox_bucket = config.outbox_bucket
        self._event_broadcaster = event_broadcaster
        self._drs_object_dao = drs_object_dao
        self._object_storage = object_storage

    def serve_drs_object(self, *, drs_id: str) -> DrsObjectServe:
        """
        Gets the drs object for serving, if it exists in the outbox
        """

        with Database(config=config) as database:
            try:
                db_object_info = database.get_drs_object(drs_id)
            except DrsObjectNotFoundError:  # pylint: disable=try-except-raise
                raise

        # If object exists in Database, see if it exists in outbox

        bucket_id = config.s3_outbox_bucket_id

        with ObjectStorage(config=config) as storage:

            if storage.does_object_exist(bucket_id, drs_id):

                # create presigned url
                download_url = storage.get_object_download_url(bucket_id, drs_id)

                # return DRS Object
                return DrsObjectServe(
                    file_id=drs_id,
                    self_uri=f"{config.drs_self_url}/{drs_id}",
                    size=db_object_info.size,
                    created_time=db_object_info.creation_date.isoformat(),
                    updated_time=db_object_info.creation_date.isoformat(),
                    checksums=[
                        Checksum(checksum=db_object_info.md5_checksum, type="md5")
                    ],
                    access_methods=[
                        AccessMethod(access_url=AccessURL(url=download_url), type="s3")
                    ],
                )

        # If the object does not exist, make a stage request
        make_stage_request(
            db_object_info,
            config,
        )

        return None

    def registered_new_file(self, *, file_object: models.FileToRegister):
        """Register a file as a new DRS Object."""

        # write file entry to database
        with Database(config=config) as database:
            database.register_drs_object(drs_object)

        # publish message that the drs file has been registered
        publish_object_registered(drs_object, config)

    async def handle_staged_file(self, *, file_id: str, content_id: str):
        """Handles a new staged filed corresponding to a registered DRS object.

        Args:
            file_id: The ID of the file as referenced outside of this service.
            content_id: A content intentifier (usually checksum based).
        """

        # Check if file exists in database
        try:
            drs_object = await self._drs_object_dao.get_by_id(file_id)
        except ResourceNotFoundError as error:
            raise NoCorrespondingDrsObjectError(file_id=file_id) from error

        if drs_object.content_id != content_id:
            raise ContentMissmatchError(
                drs_id=drs_object.drs_id,
                observed_content_id=content_id,
                expected_content_id=drs_object.content_id,
            )

        # Check if file is in outbox
        if not await self._object_storage.does_object_exist(
            bucket_id=self._outbox_bucket, object_id=file_id
        ):
            raise DrsObjectNotInOutbox(drs_id=drs_object.drs_id)

        # update DRS object metadata to indicate that is now in the outbox:
        updated_drs_object = drs_object.copy(update={"in_outbox": True})
        await self._drs_object_dao.update(updated_drs_object)
