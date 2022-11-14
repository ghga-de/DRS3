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

"""
Utils to customize openAPI script
"""
from typing import Any, Dict

from fastapi.openapi.utils import get_openapi

from dcs import __version__
from dcs.config import Config

config = Config()


def get_openapi_schema(api) -> Dict[str, Any]:

    """Generates a custom openapi schema for the service"""

    return get_openapi(
        title="Download Controller Service",
        version=__version__,
        description="A GA4GH drs compliant service for delivering files from S3.",
        servers=[{"url": config.api_route}],
        routes=api.routes,
    )