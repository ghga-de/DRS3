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
"""Check JWT validation functionality"""

import os

import pytest
from jwcrypto.jws import InvalidJWSSignature
from jwcrypto.jwt import JWTExpired

from dcs.core.jwt_validation import get_validated_token
from tests.fixtures.joint import get_work_order_token


def test_validation_happy():
    """Test decoding/validation path"""

    signed_token, pubkey = get_work_order_token()

    decoded = get_validated_token(token=signed_token, signing_pubkey=pubkey)
    assert decoded["name"] and decoded["name"] == "John Doe"
    assert decoded["role"] and decoded["role"] == "admin"


def test_validation_sad():
    """Test for expected error scenarios in decoding/validation path"""

    signed_token, pubkey = get_work_order_token()
    _, wrong_pubkey = get_work_order_token()

    # test validation failure with wrong key
    with pytest.raises(InvalidJWSSignature):
        get_validated_token(token=signed_token, signing_pubkey=wrong_pubkey)

    # mess with payload
    payload = signed_token.split(".")[1]
    invalid_token = signed_token.replace(payload, payload[-1] + payload[1:])

    with pytest.raises(InvalidJWSSignature):
        get_validated_token(token=invalid_token, signing_pubkey=pubkey)

    no_signature_token = ".".join(signed_token.split(".")[:2])
    with pytest.raises(ValueError, match="Token format unrecognized"):
        get_validated_token(token=no_signature_token, signing_pubkey=pubkey)

    with pytest.raises(ValueError, match="Token format unrecognized"):
        get_validated_token(token=os.urandom(32).hex(), signing_pubkey=pubkey)

    signed_token, pubkey = get_work_order_token(valid_seconds=-60)
    with pytest.raises(JWTExpired):
        get_validated_token(token=signed_token, signing_pubkey=pubkey)
