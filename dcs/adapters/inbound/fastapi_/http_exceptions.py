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


"""A collection of http exceptions."""

from httpyexpect.server import HttpCustomExceptionBase
from pydantic import BaseModel


class HttpDBInteractionError(HttpCustomExceptionBase):
    """Thrown when interaction with a database produces an error"""

    exception_id = "dbInteractionError"

    def __init__(self, *, description: str, status_code: int = 500):
        """Construct message and init the exception."""
        super().__init__(status_code=status_code, description=description, data={})


class HttpDownloadLinkExpiredError(HttpCustomExceptionBase):
    """Thrown when a download was requested after its expiration datetime has been reached"""

    exception_id = "downloadLinkExpiredError"

    def __init__(self, *, status_code: int = 410):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code, description="Download link has expired", data={}
        )


class HttpDownloadNotFoundError(HttpCustomExceptionBase):
    """Thrown when the download_id or signature do not match an existing download entry"""

    exception_id = "downloadNotFoundError"

    def __init__(self, status_code: int = 404):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="No valid download found for the given URL",
            data={},
        )


class HttpEnvelopeNotFoundError(HttpCustomExceptionBase):
    """
    Thrown when envelope data unexpectedly is not found.
    As this would only happen due to database inconsistencies, this is a server error
    """

    exception_id = "envelopeNotFoundError"

    def __init__(self, *, status_code: int = 500):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="Envelope for the given download could not be found",
            data={},
        )


class HttpExternalAPIError(HttpCustomExceptionBase):
    """Thrown when communication with an external API produces an error"""

    exception_id = "externalAPIError"

    def __init__(self, *, description: str, status_code: int = 500):
        """Construct message and init the exception."""
        super().__init__(status_code=status_code, description=description, data={})


class HttpObjectNotFoundError(HttpCustomExceptionBase):
    """Thrown when a file with given ID could not be found."""

    exception_id = "noSuchObject"

    class DataModel(BaseModel):
        """Model for exception data"""

        object_id: str

    def __init__(self, *, object_id: str, status_code: int = 404):
        """Construct message and init the exception."""
        super().__init__(
            status_code=status_code,
            description="The requested DrsObject wasn't found",
            data={"object_id": object_id},
        )


class HttpRangeParsingError(HttpCustomExceptionBase):
    """Thrown when parsing a download request range header fails"""

    exception_id = "rangeParsingError"

    def __init__(self, *, message: str, status_code: int = 400):
        """Construct message and init the exception."""
        super().__init__(status_code=status_code, description=message, data={})
