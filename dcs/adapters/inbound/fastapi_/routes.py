# Copyright 2022 Universität Tübingen, DKFZ and EMBL
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
Module containing the main FastAPI router and all route functions.
"""

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from dcs.adapters.inbound.fastapi_ import http_exceptions, http_responses
from dcs.container import Container
from dcs.core.models import DrsObjectWithAccess
from dcs.ports.inbound.data_repository import DataRepositoryPort

router = APIRouter()


class ObjectNotInOutboxModel(BaseModel):
    """Pydantic model for 202 Response"""

    message: str


RESPONSES = {
    "noSuchObject": {
        "description": (
            "Exceptions by ID:"
            + "\n- noSuchUpload: The requested DrsObject wasn't found"
        ),
        "model": http_exceptions.HttpObjectNotFoundError.get_body_model(),
    },
    "objectNotInOutbox": {
        "description": (
            "Exceptions by ID:"
            + "\n- objectNotInOutbox: The requested DrsObject is not staged in"
            + "the outbox. Retry later"
        ),
        "model": ObjectNotInOutboxModel,
    },
}


@router.get(
    "/files/{file_id}",
    summary="Get file metadata including the current upload attempt.",
    operation_id="getDrsObject",
    status_code=status.HTTP_200_OK,
    response_model=DrsObjectWithAccess,
    response_description="File metadata including the current upload attempt",
    responses={
        status.HTTP_202_ACCEPTED: RESPONSES["objectNotInOutbox"],
        status.HTTP_404_NOT_FOUND: RESPONSES["noSuchObject"],
    },
)
@inject
async def get_drs_object(
    object_id: str,
    data_repository: DataRepositoryPort = Depends(Provide[Container.data_repository]),
):
    """
    Get info about a ``DrsObject``.
    """

    try:
        drs_object = await data_repository.access_drs_object(drs_id=object_id)
        return drs_object

    except data_repository.RetryAccessLaterError:
        # tell client to retry after 5 minutes
        return http_responses.HttpObjectNotInOutboxResponse(retry_after=300)

    except data_repository.DrsObjectNotFoundError as object_not_found_error:
        raise http_exceptions.HttpObjectNotFoundError(
            object_id=object_id
        ) from object_not_found_error
