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
"""TODO"""

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


class HttpDownloadEndpointErrorModel(BaseModel):
    """TODO"""

    __root__: Union[  # type: ignore
        HttpEnvelopeNotFoundErrorModel, HttpExternalApiErrorModel
    ] = Field(...)


class HttpObjectEndpointErrorModel(BaseModel):
    """TODO"""

    __root__: Union[  # type: ignore
        HttpDBInteractionErrorModel, HttpExternalApiErrorModel
    ] = Field(...)
