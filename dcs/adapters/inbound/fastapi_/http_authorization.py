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
"""Authoriaztion specific code for FastAPI"""

__all__ = ["require_work_order_token"]


from dependency_injector.wiring import Provide, inject
from fastapi import Depends, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from ghga_service_commons.auth.context import AuthContextProtocol
from ghga_service_commons.auth.policies import require_auth_context_using_credentials

from dcs.container import Container
from dcs.core.auth_policies import WorkOrderContext


@inject
async def require_access_token(
    credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=True)),
    auth_provider: AuthContextProtocol[WorkOrderContext] = Depends(
        Provide[Container.auth_provider]
    ),
) -> str:
    """Require an access token using FastAPI."""
    return require_auth_context_using_credentials(
        credentials=credentials, auth_provider=auth_provider
    )


require_work_order_token = Security(require_access_token)
