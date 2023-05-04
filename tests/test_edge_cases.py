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

import pytest
from fastapi import status
from hexkit.providers.mongodb.testutils import mongodb_fixture  # noqa: F401
from hexkit.providers.s3.testutils import file_fixture  # noqa: F401
from hexkit.providers.s3.testutils import s3_fixture  # noqa: F401

from tests.fixtures.joint import *  # noqa: F403


@pytest.mark.asyncio
async def test_get_health(joint_fixture: JointFixture):  # noqa: F811, F405
    """Test the GET /health endpoint"""

    response = await joint_fixture.rest_client.get("/health")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "OK"}


@pytest.mark.asyncio
async def test_access_non_existing(
    monkeypatch, joint_fixture: JointFixture  # noqa F811
):
    """Checks that requesting access to a non-existing DRS object fails with the
    expected exception."""

    # request access to non existing DRS object:
    work_order_token, pubkey = get_work_order_token()  # noqa: F405

    # test with missing authorization header
    response = await joint_fixture.rest_client.get("/objects/my-non-existing-id")
    assert response.status_code == status.HTTP_403_FORBIDDEN

    # test with authorization header but wrong pubkey
    response = await joint_fixture.rest_client.get(
        "/objects/my-non-existing-id",
        headers={"Authorization": f"Bearer {work_order_token}"},
    )
    assert response.status_code == status.HTTP_403_FORBIDDEN

    with monkeypatch.context() as patch:
        patch.setattr(
            "dcs.core.data_repository.DataRepository._get_signing_pubkey",
            lambda self: pubkey,
        )
        # test with correct authorization header but wrong object_id
        response = await joint_fixture.rest_client.get(
            "/objects/my-non-existing-id",
            headers={"Authorization": f"Bearer {work_order_token}"},
        )
        assert response.status_code == status.HTTP_404_NOT_FOUND
