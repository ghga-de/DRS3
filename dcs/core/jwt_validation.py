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

import json
from typing import Union

from jwcrypto import jwk, jwt


def get_validated_token(token: str, signing_pubkey: str) -> dict[str, str]:
    """Validate token and return decoded information as dict"""
    # create JWK from signing public key
    pem = wrap_in_pem(signing_pubkey)
    key = jwk.JWK.from_pem(data=pem)

    decoded_token = jwt.JWT(jwt=token, key=key, expected_type="JWS")
    return json.loads(decoded_token.claims)


def wrap_in_pem(key: Union[bytes, str]) -> bytes:
    """Add pem conformant parts for parsing with jwcrypto"""

    if isinstance(key, bytes):
        key = key.decode("utf-8")

    pem = f"-----BEGIN PUBLIC KEY-----\n{key}\n-----END PUBLIC KEY-----\n"
    return bytes(pem, encoding="utf-8")
