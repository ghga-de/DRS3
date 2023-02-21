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
"""Custom composite response models"""

from typing import Union

from pydantic import BaseModel, Field

from dcs.adapters.inbound.fastapi_.http_exceptions import (
    HttpDBInteractionError,
    HttpEnvelopeNotFoundError,
    HttpExternalAPIError,
)

HttpExternalApiErrorModel = HttpExternalAPIError.get_body_model()
HttpEnvelopeNotFoundErrorModel = HttpEnvelopeNotFoundError.get_body_model()
HttpDBInteractionErrorModel = HttpDBInteractionError.get_body_model()


class DeliveryDelayedModel(BaseModel):
    """Pydantic model for 202 Response. Empty, since 202 has no body."""


class RedirectResponseModel(BaseModel):
    """Response model for the objectstorage redirect"""

    url: str


class HttpDownloadEndpointErrorModel(BaseModel):
    """Pydantic model for possible 500 responses for the download endpoint"""

    __root__: Union[  # type: ignore
        HttpEnvelopeNotFoundErrorModel, HttpExternalApiErrorModel
    ] = Field(...)


class HttpObjectEndpointErrorModel(BaseModel):
    """Pydantic model for possible 500 responses for the download endpoint"""

    __root__: Union[  # type: ignore
        HttpDBInteractionErrorModel, HttpExternalApiErrorModel
    ] = Field(...)


class ObjectPartWithEnvelopeModel(BaseModel):
    """Response model for first object part with envelope"""

    media_type: str
    content: bytes
