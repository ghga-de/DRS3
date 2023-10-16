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

"""Module hosting the dependency injection container."""

from collections.abc import AsyncGenerator, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Callable

from fastapi import FastAPI
from ghga_service_commons.api import configure_app
from ghga_service_commons.auth.jwt_auth import JWTAuthContextProvider
from hexkit.protocols.dao import DaoFactoryProtocol
from hexkit.protocols.eventpub import EventPublisherProtocol
from hexkit.protocols.objstorage import ObjectStorageProtocol
from hexkit.providers.akafka import KafkaEventPublisher, KafkaEventSubscriber
from hexkit.providers.mongodb import MongoDbDaoFactory
from typing_extensions import TypeAlias

from dcs.adapters.inbound.event_sub import EventSubTranslator
from dcs.adapters.inbound.fastapi_ import dummies
from dcs.adapters.inbound.fastapi_.custom_openapi import get_openapi_schema
from dcs.adapters.inbound.fastapi_.routes import router
from dcs.adapters.outbound.dao import DrsObjectDaoConstructor
from dcs.adapters.outbound.event_pub import EventPubTranslator
from dcs.adapters.outbound.s3 import S3ObjectStorage
from dcs.config import Config
from dcs.core.auth_policies import WorkOrderContext
from dcs.core.data_repository import DataRepository
from dcs.ports.inbound.data_repository import DataRepositoryPort
from dcs.ports.outbound.dao import DrsObjectDaoPort
from dcs.ports.outbound.event_pub import EventPublisherPort
from dcs.utils import DependencyResolver


@dataclass
class CoreDependencies:
    """DI Container for the core application and outbound adapters.
    Inbound adapters are managed separately.

    Instances of this class are returned from the dependency resolution process.
    They contain initialized resources but not the construction logic.
    """

    config: Config

    # outbound providers:
    dao_factory: DaoFactoryProtocol
    event_pub_provider: EventPublisherProtocol

    # outbound translators:
    drs_object_dao: DrsObjectDaoPort
    event_publisher: EventPublisherPort

    # outbound adapters (not following the triple hexagonal translator/provider):
    object_storage: ObjectStorageProtocol

    # domain/core components:
    data_repository: DataRepositoryPort


@asynccontextmanager
async def prepare_core_dependencies(
    *, config: Config
) -> AsyncGenerator[CoreDependencies, None]:
    """Constructs and initializes all core components and their outbound dependencies.
    This is a context manager returns a container with all initialized resources upon
    __enter__. The resources are teared down upon __exit__.

    """
    async with KafkaEventPublisher.construct(config=config) as event_pub_provider:
        dao_factory = MongoDbDaoFactory(config=config)
        drs_object_dao = await DrsObjectDaoConstructor.construct(
            dao_factory=dao_factory
        )
        event_publisher = EventPubTranslator(config=config, provider=event_pub_provider)
        object_storage = S3ObjectStorage(config=config)
        data_repository = DataRepository(
            drs_object_dao=drs_object_dao,
            object_storage=object_storage,
            event_publisher=event_publisher,
            config=config,
        )
        yield CoreDependencies(
            config=config,
            dao_factory=dao_factory,
            event_pub_provider=event_pub_provider,
            drs_object_dao=drs_object_dao,
            event_publisher=event_publisher,
            object_storage=object_storage,
            data_repository=data_repository,
        )


CoreDependencyResolver: TypeAlias = DependencyResolver[Config, CoreDependencies]
OutboxCleaner: TypeAlias = Callable[[], Coroutine[None, None, None]]


def get_configured_rest_app(*, config: Config) -> FastAPI:
    """Create and configure a REST API application."""
    app = FastAPI()
    app.include_router(router)
    configure_app(app, config=config)

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi_schema(app)
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore [method-assign]

    return app


@asynccontextmanager
async def prepare_rest_app(
    *,
    config: Config,
    prep_core_deps: CoreDependencyResolver = prepare_core_dependencies,
) -> AsyncGenerator[FastAPI, None]:
    """Construct and initialize an REST API app along with all its dependencies."""
    app = get_configured_rest_app(config=config)

    async with (
        prep_core_deps(config=config) as core_dependencies,
        JWTAuthContextProvider.construct(
            config=config, context_class=WorkOrderContext
        ) as auth_context,
    ):
        app.dependency_overrides[dummies.auth_provider] = lambda: auth_context
        app.dependency_overrides[
            dummies.data_repo_port
        ] = lambda: core_dependencies.data_repository
        yield app


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
    prep_core_deps: CoreDependencyResolver = prepare_core_dependencies,
) -> AsyncGenerator[KafkaEventSubscriber, None]:
    """Construct and initialize an event subscriber with all its dependencies."""
    async with prep_core_deps(config=config) as core_dependencies:
        event_sub_translator = EventSubTranslator(
            data_repository=core_dependencies.data_repository,
            config=config,
        )

        async with KafkaEventSubscriber.construct(
            config=config, translator=event_sub_translator
        ) as event_subscriber:
            yield event_subscriber


@asynccontextmanager
async def prepare_outbox_cleaner(
    *,
    config: Config,
    prep_core_deps: CoreDependencyResolver = prepare_core_dependencies,
) -> AsyncGenerator[OutboxCleaner, None]:
    """Construct and initialize a coroutine that cleans the outbox once invoked."""
    async with prepare_core_dependencies(config=config) as core_dependencies:
        yield core_dependencies.data_repository.cleanup_outbox
