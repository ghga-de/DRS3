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

"""Interface for broadcasting events to other services."""

from pydantic import BaseSettings, Field

from hexkit.protocols.eventpub import EventPublisherProtocol

from dcs.core import models
from dcs.ports.outbound.event_pub import EventPublisherPort


class EventPubTanslatorConfig(BaseSettings):
    """Config for publishing file upload-related events."""

    download_served_event_topic: str = Field(
        "downloads",
        description=(
            "Name of the topic to publish an event that informs about a DRS object being accessed."
        ),
    )
    download_served_event_type: str = Field(
        "download_served",
        description="The type to use for event that informs about a DRS object being accessed.",
    )
    unstaged_download_event_topic: str = Field(
        "downloads",
        description=(
            "Name of the topic to publish an event that informs about an DRS object being"
            + " requested that is not yet available in the outbox."
        ),
    )
    download_served_event_type: str = Field(
        "unstaged_download_requested",
        description=(
            "The type to use for event that informsabout a DRS object being"
            + " requested that is not yet available in the outbox."
        ),
    )
    file_registered_event_topic: str = Field(
        "downloads",
        description=(
            "Name of the topic to publish an event that informs about a new file "
            + " being registered"
        ),
    )
    file_registered_event_type: str = Field(
        "file_registered",
        description=(
            "The type to use for event that informs about a new file being registered"
        ),
    )


class EventPubTranslator(EventPublisherPort):
    """A translator (according to the triple hexagonal architecture) for publishing
    events using the EventPublisherProtocol."""

    def __init__(
        self, *, config: EventPubTanslatorConfig, provider: EventPublisherProtocol
    ):
        """Initialize with a suitable protocol provider."""

        self._config = config
        self._provider = provider

    async def download_served(self, *, drs_object: models.DrsObjectWithUri) -> None:
        """Communicate the event of an download being served. This can be relevant for
        auditing purposes."""

    async def unstaged_download_requested(
        self, *, drs_object: models.DrsObjectWithUri
    ) -> None:
        """Communicates the event that a download was requested for a DRS object, that
        is not yet available in the outbox."""

    async def file_registered(self, *, drs_object: models.DrsObjectWithUri) -> None:
        """Communicates the event that a file has been registered."""
