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


"""
Module containing the main FastAPI router and all route functions.
"""

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Header, status
from pydantic import BaseModel

from dcs.adapters.inbound.fastapi_ import http_exceptions, http_responses, ranges
from dcs.container import Container
from dcs.core.models import DrsObjectWithAccess
from dcs.ports.inbound.data_repository import DataRepositoryPort

router = APIRouter()


class DeliveryDelayedModel(BaseModel):
    """Pydantic model for 202 Response. Empty, since 202 has no body."""


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
            "The operation is delayed and will continue asynchronously. "
            + "The client should retry this same request after the delay "
            + "specified by Retry-After header."
        ),
        "model": DeliveryDelayedModel,
    },
    "noSuchDownload": {
        "description": (
            "Exceptions by ID:"
            + "\n- noSuchDownload: The requested download was not found"
        ),
        "model": None,
    },
    "downloadExpired": {
        "description": (
            "Exceptions by ID:"
            + "\n- downloadExpired: The provided download URL is no longer valid"
        ),
        "model": None,
    },
}


@router.get(
    "/health",
    summary="health",
    tags=["DownloadControllerService"],
    status_code=status.HTTP_200_OK,
)
async def health():
    """Used to test if this service is alive"""

    return {"status": "OK"}


@router.get(
    "/objects/{object_id}",
    summary="Returns object metadata, and a list of access methods that can be used "
    + "to fetch object bytes.",
    operation_id="getDrsObject",
    tags=["DownloadControllerService"],
    status_code=status.HTTP_200_OK,
    response_model=DrsObjectWithAccess,
    response_description="The DrsObject was found successfully.",
    responses={
        status.HTTP_202_ACCEPTED: RESPONSES["objectNotInOutbox"],
        status.HTTP_404_NOT_FOUND: RESPONSES["noSuchObject"],
    },
)
@inject
async def get_drs_object(
    object_id: str,
    public_key: str = Header(...),
    data_repository: DataRepositoryPort = Depends(Provide[Container.data_repository]),
):
    """
    Get info about a ``DrsObject``.
    """

    try:
        drs_object = await data_repository.access_drs_object(
            drs_id=object_id, public_key=public_key
        )
        return drs_object

    except data_repository.RetryAccessLaterError as retry_later_error:
        # tell client to retry after 5 minutes
        return http_responses.HttpObjectNotInOutboxResponse(
            retry_after=retry_later_error.retry_after
        )

    except data_repository.DrsObjectNotFoundError as object_not_found_error:
        raise http_exceptions.HttpObjectNotFoundError(
            object_id=object_id
        ) from object_not_found_error

    except (
        data_repository.APICommunicationError,
        data_repository.SecretNotFoundError,
        data_repository.UnexpectedAPIResponseError,
    ) as external_api_error:
        raise http_exceptions.HttpExternalAPIError(
            description=str(external_api_error)
        ) from external_api_error
    except data_repository.DuplicateEntryError as db_interaction_error:
        raise http_exceptions.HttpDBInteractionError(
            description=str(db_interaction_error)
        ) from db_interaction_error


@router.get(
    "downloads/{download_id}/?signature={signature}",
    summary="",
    operation_id="serveDownload",
    tags=["DownloadControllerService"],
    status_code=status.HTTP_206_PARTIAL_CONTENT,
    response_model=None,
    response_description="Succesfully retrieved .",
    responses={},
)
@inject
async def get_download(
    download_id: str,
    signature: str,
    range: str = Header(...),
    data_repository: DataRepositoryPort = Depends(Provide[Container.data_repository]),
):
    """
    Retrieve
    """

    # check download information and retreive envelope data
    try:
        envelope_data, file_id = await data_repository.validate_download_information(
            download_id=download_id, signature=signature
        )
    except data_repository.DownloadNotFoundError as error:
        raise http_exceptions.HttpDownloadNotFoundError() from error
    except data_repository.DonwloadLinkExpired as error:
        raise http_exceptions.HttpDownloadLinkExpiredError() from error
    except data_repository.EnvelopeNotFoundError as error:
        raise http_exceptions.HttpEnvelopeNotFoundError() from error

    offset = envelope_data.offset
    parsed_range = ranges.parse_header(range_header=range, offset=offset)

    if parsed_range[0] <= offset:
        # envelope in range
        try:
            object_part = await data_repository.serve_envelope_part(
                object_id=file_id,
                parsed_range=parsed_range,
                envelope_header=envelope_data.header,
            )
        except data_repository.APICommunicationError as error:
            raise http_exceptions.HttpExternalAPIError(description=str(error))

        # TODO: headers for 206 response
        return http_responses.HttpObjectPartWithEnvelopeResponse(
            content=object_part, headers={}
        )

    # envelope not in range
    redirect_url, redirect_header = await data_repository.serve_redirect(
        object_id=file_id, parsed_range=parsed_range
    )
    return http_responses.HttpDownloadRedirectResponse(
        url=redirect_url, redirect_header=redirect_header
    )
