# ModSettings

*Modifies the orginal Settings class provided by the user*

## Properties

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
