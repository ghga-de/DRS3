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

"""A collection of http responses."""

from typing import Mapping

from fastapi.responses import JSONResponse, RedirectResponse, Response


class HttpDownloadRedirectResponse(RedirectResponse):
    """Returns S3 download link with adjusted range in header"""

    response_id = "downloadRedirected"

    def __init__(
        self, url: str, redirect_header: dict[str, str], status_code: int = 301
    ):
        """Construct message and init the response."""

        super().__init__(url=url, status_code=status_code, headers=redirect_header)


class HttpObjectNotInOutboxResponse(JSONResponse):
    """
    Returned, when a file has not been staged to the outbox yet.
    """

    response_id = "objectNotInOutbox"

    def __init__(
        self,
        *,
        status_code: int = 202,
        retry_after: int = 300,
    ):
        """Construct message and init the response."""

        headers = {"Retry-After": str(retry_after)}
        super().__init__(content=None, status_code=status_code, headers=headers)


class HttpObjectPartWithEnvelopeResponse(Response):
    """Returned when the requested range lies within the envelope"""

    response_id = "objectPartWithEnvelope"

    def __init__(
        self,
        content: bytes,
        headers: Mapping[str, str],
        status_code: int = 206,
    ):
        """Construct message and init the response."""
        media_type = "binary/octet-stream"
        super().__init__(content, status_code, headers, media_type)
