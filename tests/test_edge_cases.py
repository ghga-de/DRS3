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

"""Tests edge cases not covered by the typical journey test."""

import re

import httpx
import pytest
from fastapi import status
from pytest_httpx import HTTPXMock, httpx_mock  # noqa: F401

from tests.fixtures.joint import *  # noqa: F403
from tests.fixtures.joint import ConfigErrorFixture, JointFixture
from tests.fixtures.mock_api.app import router
from tests.fixtures.utils import generate_token_signing_keys, generate_work_order_token

unintercepted_hosts: list[str] = ["localhost"]


@pytest.fixture
def non_mocked_hosts() -> list:
    """Fixture used by httpx_mock to determine which requests to intercept

    We only want to intercept calls to the EKSS API, so this list will include
    localhost and the host from the S3 fixture's connection URL.
    """
    return unintercepted_hosts


@pytest.mark.asyncio
async def test_get_health(joint_fixture: JointFixture):
    """Test the GET /health endpoint"""
    response = await joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


@pytest.mark.asyncio
async def test_access_non_existing(joint_fixture: JointFixture):
    """Checks that requesting access to a non-existing DRS object fails with the
    expected exception.
    """
    file_id = "my-non-existing-id"

    work_order_token = generate_work_order_token(file_id=file_id, jwk=joint_fixture.jwk)
    wrong_jwk = generate_token_signing_keys()
    wrong_work_order_token = generate_work_order_token(file_id=file_id, jwk=wrong_jwk)

    # test with missing authorization header
    # (should not expose whether the file with the given id exists or not)
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # test with authorization header but wrong pubkey
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
        timeout=5,
        headers={"Authorization": f"Bearer {wrong_work_order_token}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # test with correct authorization header but wrong object_id
    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}",
        timeout=5,
        headers={"Authorization": f"Bearer {work_order_token}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND

    response = await joint_fixture.rest_client.get(
        f"/objects/{file_id}/envelopes",
        timeout=5,
        headers={"Authorization": f"Bearer {work_order_token}"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_deletion_config_error(
    config_error_fixture: ConfigErrorFixture, httpx_mock: HTTPXMock  # noqa: F811
):
    """Simulate a deletion request for a file with an unconfigured storage alias."""
    # explicitly handle ekss API calls (and name unintercepted hosts above)
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{config_error_fixture.joint.config.ekss_base_url}.*"),
    )

    data_repository = config_error_fixture.joint.data_repository
    with pytest.raises(data_repository.StorageAliasNotConfiguredError):
        await data_repository.delete_file(file_id=config_error_fixture.file_id)


@pytest.mark.asyncio
async def test_drs_config_error(
    config_error_fixture: ConfigErrorFixture,
    httpx_mock: HTTPXMock,  # noqa: F811
):
    """Test DRS endpoint for a storage alias that is not configured"""
    # generate work order token
    work_order_token = generate_work_order_token(
        file_id=config_error_fixture.file_id,
        jwk=config_error_fixture.joint.jwk,
        valid_seconds=120,
    )

    # modify default headers:
    config_error_fixture.joint.rest_client.headers = httpx.Headers(
        {"Authorization": f"Bearer {work_order_token}"}
    )

    # explicitly handle ekss API calls (and name unintercepted hosts above)
    httpx_mock.add_callback(
        callback=router.handle_request,
        url=re.compile(rf"^{config_error_fixture.joint.config.ekss_base_url}.*"),
    )

    drs_id = config_error_fixture.file_id
    response = await config_error_fixture.joint.rest_client.get(
        f"/objects/{drs_id}", timeout=5
    )
    assert response.status_code == 500
