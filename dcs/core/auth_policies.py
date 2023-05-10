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
"""Supported authentication policies for endpoints"""

from typing import Literal, Union

from pydantic import BaseModel, EmailStr, Field


class WorkOrderToken(BaseModel):
    """Auth context for a work order token used for downloads"""

    type: Union[Literal["download"], Literal["upload"]] = Field(
        ..., title="Type", help="Work type"
    )
    file_id: str = Field(
        ...,
        title="File ID",
        help="The ID of the file that shall be downloaded or uploaded",
    )
    user_id: str = Field(..., title="User ID", help="The internal ID of the user")
    user_public_crypt4gh_key: str = Field(
        ..., help="Base64 encoded Crypt4GH public key of the user"
    )
    full_user_name: str = Field(
        ...,
        title="Full user name",
        help="The full name of the user (with academic title)",
    )
    email: EmailStr = Field(..., title="E-Mail", help="The email address of the user")

    def matches_type_and_file_id(self, *, file_id: str):
        """Validate token target file id and endpoint type match expectations"""

        if self.type != "download":
            raise ValueError("Got token for wrong endpoint type.")
        if self.file_id != file_id:
            raise ValueError("Got token for wrong file_id.")
