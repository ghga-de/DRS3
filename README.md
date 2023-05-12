![tests](https://github.com/ghga-de/dcs/actions/workflows/unit_and_int_tests.yaml/badge.svg)
[![codecov](https://codecov.io/gh/ghga-de/dcs/branch/main/graph/badge.svg?token=GYH99Y71CK)](https://codecov.io/gh/ghga-de/dcs)
# Download Controller Service

Download Controller Service - a GA4GH drs compliant service for delivering files from S3

## Description:

<!-- Please adapt. -->

This service implements the GA4GH DRS, while providing the option to serve files from localstack S3.


## Installation
We recommend using the provided Docker container.

A pre-build version is available at [docker hub](https://hub.docker.com/repository/docker/ghga/download-controller-service):
```bash
# Please feel free to choose the version as needed:
docker pull ghga/download-controller-service:<version>
```

Or you can build the container yourself from the [`./Dockerfile`](./Dockerfile):
```bash
# Execute in the repo's root dir:
docker build -t ghga/download-controller-service:<version> .
```

For production-ready deployment, we recommend using Kubernetes, however,
for simple use cases, you could execute the service using docker
on a single server:
```bash
# The entrypoint is preconfigured:
docker run -p 8080:8080 ghga/download-controller-service:<version> --help
```

If you prefer not to use containers, you may install the service from source:
```bash
# Execute in the repo's root dir:
pip install .

# To run the service:
dcs --help
```

## Configuration:
### Parameters:

The service requires the following configuration parameters:
- **`files_to_register_topic`** *(string)*: The name of the topic to receive events informing about new files that shall be made available for download.

- **`files_to_register_type`** *(string)*: The type used for events informing about new files that shall be made available for download.

- **`download_served_event_topic`** *(string)*: Name of the topic used for events indicating that a download of a specified file happened.

- **`download_served_event_type`** *(string)*: The type used for event indicating that a download of a specified file happened.

- **`unstaged_download_event_topic`** *(string)*: Name of the topic used for events indicating that a download was requested for a file that is not yet available in the outbox.

- **`unstaged_download_event_type`** *(string)*: The type used for event indicating that a download was requested for a file that is not yet available in the outbox.

- **`file_registered_event_topic`** *(string)*: Name of the topic used for events indicating that a file has been registered for download.

- **`file_registered_event_type`** *(string)*: The type used for event indicating that that a file has been registered for download.

- **`service_name`** *(string)*: Default: `dcs`.

- **`service_instance_id`** *(string)*: A string that uniquely identifies this instance across all instances of this service. A globally unique Kafka client ID will be created by concatenating the service_name and the service_instance_id.

- **`kafka_servers`** *(array)*: A list of connection strings to connect to Kafka bootstrap servers.

  - **Items** *(string)*

- **`db_connection_str`** *(string)*: MongoDB connection string. Might include credentials. For more information see: https://naiveskill.com/mongodb-connection-string/.

- **`db_name`** *(string)*: Name of the database located on the MongoDB server.

- **`outbox_bucket`** *(string)*

- **`drs_server_uri`** *(string)*: The base of the DRS URI to access DRS objects. Has to start with 'drs://' and end with '/'.

- **`retry_access_after`** *(integer)*: When trying to access a DRS object that is not yet in the outbox, instruct to retry after this many seconds. Default: `120`.

- **`ekss_base_url`** *(string)*: URL containing host and port of the EKSS endpoint to retrieve personalized envelope from.

- **`presigned_url_expires_after`** *(integer)*: Expiration time in seconds for presigned URLS. Positive integer required.

- **`s3_endpoint_url`** *(string)*: URL to the S3 API.

- **`s3_access_key_id`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`s3_secret_access_key`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`s3_session_token`** *(string)*: Part of credentials for login into the S3 service. See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html.

- **`aws_config_ini`** *(string)*: Path to a config file for specifying more advanced S3 parameters. This should follow the format described here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file.

- **`host`** *(string)*: IP of the host. Default: `127.0.0.1`.

- **`port`** *(integer)*: Port to expose the server on the specified host. Default: `8080`.

- **`log_level`** *(string)*: Controls the verbosity of the log. Must be one of: `['critical', 'error', 'warning', 'info', 'debug', 'trace']`. Default: `info`.

- **`auto_reload`** *(boolean)*: A development feature. Set to `True` to automatically reload the server upon code changes. Default: `False`.

- **`workers`** *(integer)*: Number of workers processes to run. Default: `1`.

- **`api_root_path`** *(string)*: Root path at which the API is reachable. This is relative to the specified host and port. Default: `/`.

- **`openapi_url`** *(string)*: Path to get the openapi specification in JSON format. This is relative to the specified host and port. Default: `/openapi.json`.

- **`docs_url`** *(string)*: Path to host the swagger documentation. This is relative to the specified host and port. Default: `/docs`.

- **`cors_allowed_origins`** *(array)*: A list of origins that should be permitted to make cross-origin requests. By default, cross-origin requests are not allowed. You can use ['*'] to allow any origin.

  - **Items** *(string)*

- **`cors_allow_credentials`** *(boolean)*: Indicate that cookies should be supported for cross-origin requests. Defaults to False. Also, cors_allowed_origins cannot be set to ['*'] for credentials to be allowed. The origins must be explicitly specified.

- **`cors_allowed_methods`** *(array)*: A list of HTTP methods that should be allowed for cross-origin requests. Defaults to ['GET']. You can use ['*'] to allow all standard methods.

  - **Items** *(string)*

- **`cors_allowed_headers`** *(array)*: A list of HTTP request headers that should be supported for cross-origin requests. Defaults to []. You can use ['*'] to allow all headers. The Accept, Accept-Language, Content-Language and Content-Type headers are always allowed for CORS requests.

  - **Items** *(string)*

- **`api_route`** *(string)*: Default: `/ga4gh/drs/v1`.


### Usage:

A template YAML for configurating the service can be found at
[`./example-config.yaml`](./example-config.yaml).
Please adapt it, rename it to `.dcs.yaml`, and place it into one of the following locations:
- in the current working directory were you are execute the service (on unix: `./.dcs.yaml`)
- in your home directory (on unix: `~/.dcs.yaml`)

The config yaml will be automatically parsed by the service.

**Important: If you are using containers, the locations refer to paths within the container.**

All parameters mentioned in the [`./example-config.yaml`](./example-config.yaml)
could also be set using environment variables or file secrets.

For naming the environment variables, just prefix the parameter name with `dcs_`,
e.g. for the `host` set an environment variable named `dcs_host`
(you may use both upper or lower cases, however, it is standard to define all env
variables in upper cases).

To using file secrets please refer to the
[corresponding section](https://pydantic-docs.helpmanual.io/usage/settings/#secret-support)
of the pydantic documentation.

# HTTP API
An OpenAPI specification for this service can be found [here](./openapi.yaml).

## Architecture and Design:
<!-- Please adapt. -->

This is a Python-based service following the Triple Hexagonal Architecture pattern.
It uses protocol/provider pairs and dependency injection mechanisms provided by the
[hexkit](https://github.com/ghga-de/hexkit) library.


## Development
For setting up the development environment, we rely on the
[devcontainer feature](https://code.visualstudio.com/docs/remote/containers) of vscode
in combination with Docker Compose.

To use it, you have to have Docker Compose as well as vscode with its "Remote - Containers"
extension (`ms-vscode-remote.remote-containers`) installed.
Then open this repository in vscode and run the command
`Remote-Containers: Reopen in Container` from the vscode "Command Palette".

This will give you a full-fledged, pre-configured development environment including:
- infrastructural dependencies of the service (databases, etc.)
- all relevant vscode extensions pre-installed
- pre-configured linting and auto-formating
- a pre-configured debugger
- automatic license-header insertion

Moreover, inside the devcontainer, a convenience commands `dev_install` is available.
It installs the service with all development dependencies, installs pre-commit.

The installation is performed automatically when you build the devcontainer. However,
if you update dependencies in the [`./setup.cfg`](./setup.cfg) or the
[`./requirements-dev.txt`](./requirements-dev.txt), please run it again.

## License
This repository is free to use and modify according to the
[Apache 2.0 License](./LICENSE).
