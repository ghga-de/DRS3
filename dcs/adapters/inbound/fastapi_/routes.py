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
from fastapi import APIRouter, Depends, Header, Query, status

from dcs.adapters.inbound.fastapi_ import (
    http_exceptions,
    http_response_models,
    http_responses,
    ranges,
)
from dcs.container import Container
from dcs.core.models import DrsObjectWithAccess
from dcs.ports.inbound.data_repository import DataRepositoryPort

router = APIRouter()


RESPONSES = {
    "downloadEndpointError": {
        "description": (
            "Exceptions by ID:"
            + "\n- envelopeNotFoundError: Communication with service external APIs failed"
            + "\n- externalAPIError: Communication with service external APIs failed"
        ),
        "model": http_response_models.HttpDownloadEndpointErrorModel,
    },
    "downloadExpiredError": {
        "description": (
            "Exceptions by ID:"
            + "\n- downloadExpired: The provided download URL is no longer valid"
        ),
        "model": http_exceptions.HttpDownloadLinkExpiredError.get_body_model(),
    },
    "noSuchDownload": {
        "description": (
            "Exceptions by ID:"
            + "\n- noSuchDownload: The requested download was not found"
        ),
        "model": http_exceptions.HttpDownloadNotFoundError.get_body_model(),
    },
    "noSuchObject": {
        "description": (
            "Exceptions by ID:\n- noSuchUpload: The requested DrsObject wasn't found"
        ),
        "model": http_exceptions.HttpObjectNotFoundError.get_body_model(),
    },
    "objectEndpointError": {
        "description": (
            "Exceptions by ID:"
            + "\n- dbInteractionError: Database communication failed"
            + "\n- externalAPIError: Communication with service external APIs failed"
        ),
        "model": http_response_models.HttpObjectEndpointErrorModel,
    },
    "objectNotInOutbox": {
        "description": (
            "The operation is delayed and will continue asynchronously. "
            + "The client should retry this same request after the delay "
            + "specified by Retry-After header."
        ),
        "model": http_response_models.DeliveryDelayedModel,
    },
    "rangeParsingError": {
        "description": (
            "Exceptions by ID:"
            + "\n- rangeParsingError: Provided range header is invalid"
        ),
        "model": http_exceptions.HttpRangeParsingError.get_body_model(),
    },
    "redirectResponse": {
        "description": (),
        "model": http_response_models.RedirectResponseModel,
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
        status.HTTP_500_INTERNAL_SERVER_ERROR: RESPONSES["objectEndpointError"],
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
    "/downloads/{download_id}",
    summary="",
    operation_id="serveDownload",
    tags=["DownloadControllerService"],
    status_code=status.HTTP_206_PARTIAL_CONTENT,
    response_model=http_response_models.ObjectPartWithEnvelopeModel,
    response_description="Successfully delivered first file part.",
    responses={
        status.HTTP_301_MOVED_PERMANENTLY: RESPONSES["redirectResponse"],
        status.HTTP_400_BAD_REQUEST: RESPONSES["rangeParsingError"],
        status.HTTP_404_NOT_FOUND: RESPONSES["noSuchDownload"],
        status.HTTP_410_GONE: RESPONSES["downloadExpiredError"],
        status.HTTP_500_INTERNAL_SERVER_ERROR: RESPONSES["downloadEndpointError"],
    },
)
@inject
async def get_download(
    download_id: str,
    signature: str = Query(...),
    range: str = Header(...),  # pylint: disable=redefined-builtin
    data_repository: DataRepositoryPort = Depends(Provide[Container.data_repository]),
):
    """
    Retrieve either a bytestream of the first file part + envelope or a redirect with a
    presigned URL to the S3 object
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
    try:
        parsed_range = ranges.parse_header(range_header=range, offset=offset)
    except ranges.RangeParsingError as error:
        raise http_exceptions.HttpRangeParsingError(message=str(error)) from error

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

        # headers for 206 response
        headers = {"Content-Range": f"bytes {parsed_range[0]}-{parsed_range[1]}/*"}
        return http_responses.HttpObjectPartWithEnvelopeResponse(
            content=object_part, headers=headers
        )

    # envelope not in range
    redirect_url, redirect_header = await data_repository.serve_redirect(
        object_id=file_id, parsed_range=parsed_range
    )
    return http_responses.HttpDownloadRedirectResponse(
        url=redirect_url, redirect_header=redirect_header
    )
