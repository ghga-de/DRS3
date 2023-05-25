# Copyright 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
"""Functionality related to outbox cleanup not coverable by DataRepository"""


from dependency_injector.wiring import Provide, inject

from dcs.config import Config
from dcs.container import Container
from dcs.ports.inbound.data_repository import DataRepositoryPort


@inject
async def clean_outbox_cache(
    data_repository: DataRepositoryPort = Provide[Container.data_repository],
    config: Config = Provide[Container.config],
):
    """
    Calling the actual cleanup function with necessary arguments configured by
    dependency injector
    """
    await data_repository.cleanup_outbox(cache_timeout=config.cache_timeout)
