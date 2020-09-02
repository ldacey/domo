# -*- coding: utf-8 -*-
import requests
import json
import pendulum
from time import sleep
from urllib.parse import urlparse
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class DomoError(Exception):
    def __init__(self, error_message, code, response):
        self.error_message = error_message
        self.error_code = code
        self.response = response

    def __str__(self):
        return repr(f"{self.error_code}: {self.error_message} {self.response}")


class DomoHook(BaseHook):
    """
    Utilizes the Domo REST API to interact with and manage Domo.

    This Hook includes methods for the following APIs:
    - DataSet API (https://developer.domo.com/docs/dataset-api-reference/dataset)
    - Group API (https://developer.domo.com/docs/groups-api-reference/groups-2)
    - Stream API (https://developer.domo.com/docs/streams-api-reference/streams)
    - Page API (https://developer.domo.com/docs/page-api-reference/page)
    - User API (https://developer.domo.com/docs/users-api-reference/users-2)
    - Activity Log API (https://developer.domo.com/docs/activity-log-api-reference/activity-log)

    Some examples:
    hook = DomoHook(domo_conn_id)

    List users and groups:
    users = hook.list_users()
    groups = hook.list_groups()

    Create a stream and upload data:
    stream = hook.create_stream(name="Test", description="testing", column_schema=[{"name": "column1", "type": "STRING}], update_method="REPLACE")
    execution = hook.create_execution(stream_id=stream["id"])
    hook.upload_part(self, stream_id=["stream_id"], execution_id=execution["id"], part_id=1, csv=io.BytesIO(), gzip=True)
    hook.commit_execution(stream_id=stream["id"], execution_id=execution["id"])
    """

    def __init__(
        self,
        domo_conn_id=None,
        scope=None,
        session_pool_size=10,
        timeout_seconds=180,
        retry_limit=3,
        retry_delay=3,
        timeout=None,
    ):
        self.domo_conn_id = domo_conn_id
        self.domo_conn = self.get_connection(domo_conn_id)
        self.scope = scope
        self.session_pool_size = session_pool_size
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.token = None
        self.session = requests.Session()

    def receive_json_header(self):
        return {
            "Accept": "application/json",
        }

    def receive_csv_header(self):
        return {
            "Accept": "text/csv",
        }

    def send_json_header(self):
        headers = self.receive_json_header()
        headers["Content-Type"] = "application/json"
        return headers

    def send_csv_header(self):
        headers = self.receive_json_header()
        headers["Content-Type"] = "text/csv"
        return headers

    def send_gzip_header(self):
        headers = self.receive_json_header()
        headers["Content-Type"] = "text/csv"
        headers["Content-Encoding"] = "gzip"
        return headers

    def clean_host(self):
        host = self.domo_conn.host
        urlparse_host = urlparse(host).hostname
        if urlparse_host:
            return urlparse_host
        else:
            return host

    def request_type(
        self,
        session,
        url,
        method="GET",
        endpoint=None,
        data=None,
        params=None,
        headers=None,
        stream=True,
        auth=None,
    ):
        if method == "GET":
            return session.get(
                url, params=params, headers=headers, stream=stream, timeout=self.timeout, auth=auth
            )
        elif method == "POST":
            return session.post(
                url, data=data, params=params, headers=headers, stream=stream, timeout=self.timeout,
            )
        elif method == "PUT":
            return session.put(
                url, data=data, params=params, headers=headers, stream=stream, timeout=self.timeout,
            )
        elif method == "PATCH":
            return session.patch(
                url, data=data, params=params, headers=headers, stream=stream, timeout=self.timeout,
            )
        elif method == "DELETE":
            return session.delete(url, auth=auth, params=params)
        else:
            raise AirflowException(f"{method} is not supported")

    def response_type(self, response, headers):
        if headers["Accept"] == "text/csv":
            return response
        else:
            return response.json()

    def call_api(self, endpoint, method, headers, params=None, data=None, stream=True):
        session = self.session
        host = self.clean_host()
        url = f"https://{host}/{endpoint}"
        attempt = 1
        while True:
            response = self.request_type(
                session=session, url=url, method=method, params=params, data=data, headers=headers,
            )
            self.log.info(f"Sent {method} request to {response.url} - {response.status_code}")
            if (
                response.status_code == requests.codes["ok"]
                or response.status_code == requests.codes["created"]
            ):
                return self.response_type(response=response, headers=headers)
            elif response.status_code == requests.codes["no_content"]:
                self.log.info(f"Request was successful - {response.status_code} code returned")
                return response.status_code
            elif response.status_code == requests.codes["unauthorized"]:
                self.create_token()
            elif response.status_code >= 500 or response.status_code == requests.codes["timeout"]:
                self.log.info(f"{response.status_code} code returned. Trying again")
                pass
            else:
                raise DomoError(response.content, response.status_code, response)
            if attempt == self.retry_limit:
                raise AirflowException((f"API requests to Domo failed {self.retry_limit} times."))
            attempt += 1
            sleep(self.retry_delay)
        return response

    def create_token(self):
        """
        Generates a Domo API Access Token using Basic Authentication.
        By default, this will use the Client ID from the conn.login and
        Client Secret from the conn.password to authenticate and it will return
        a token which lasts for just under 1 hour.

        :return: Requests Session object
        """
        self.log.info(f"Logging into Domo.")
        session = self.session
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.session_pool_size, pool_maxsize=self.session_pool_size
        )
        session.mount("https://", adapter)
        host = self.clean_host()
        url = f"https://{host}/oauth/token?"
        extras = self.domo_conn.extra_dejson
        if self.scope is not None:
            scope = " ".join([i for i in self.scope])
        else:
            scope = extras.get("scope", "")
        params = {"scope": scope}
        params["grant_type"] = "client_credentials"
        response = self.request_type(
            session=session,
            url=url,
            method="GET",
            params=params,
            data=None,
            headers=None,
            auth=requests.auth.HTTPBasicAuth(self.domo_conn.login, self.domo_conn.password),
        )
        self.log.info(f"Sent POST request to {response.url}")
        response.raise_for_status()
        auth = response.json()
        self.log.info(f"Logged into {auth['domain']} as {auth['role']} with scope: {auth['scope']}")
        self.token = auth["access_token"]
        session.headers.update({"Authorization": f"Bearer {self.token}"})
        self.log.info(f"Token expires in {auth['expires_in']} seconds.")
        return session

    def handle_pagination(self, endpoint, **kwargs):
        """
        Handles the pagination for Domo GET requests and returns a generator of the results.

        r = hook.list_datasets(per_page=50, offset=0, max_records=200)
        datasets = list(r)
        dataset_ids = [x["id"] for x in r]

        :param endpoint: The Domo endpoint requests are being sent to
        :param sort: The DataSet field to sort by. Fields prefixed with a negative sign reverses the sort (i.e. '-name' does a reverse sort by the name of the DataSets)
        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. This varies per endpoint.
        :param offset: The offset location of records you retrieve. Default is 0.
        """
        params = {}
        if kwargs.get("per_page") is not None:
            per_page = kwargs["per_page"]
            params["limit"] = per_page
        if kwargs.get("max_records") is not None:
            max_records = kwargs["max_records"]
            if isinstance(max_records, int):
                per_page = min(per_page, max_records)
                self.log.info(
                    f"Retrieving a maximum of {max_records} records ({per_page} records per page)"
                )
            else:
                per_page = per_page
                self.log.info(f"Retrieving all records ({per_page} records per page)")
        if kwargs.get("sort") is not None:
            params["sort"] = kwargs["sort"]
        if kwargs.get("offset") is not None:
            params["offset"] = kwargs["offset"]
        if kwargs.get("start_time") is not None:
            params["start"] = kwargs["start_time"]
        if kwargs.get("end_time") is not None:
            params["end"] = kwargs["end_time"]
        count = 0
        results = self.call_api(
            endpoint=endpoint, method="GET", headers=self.send_json_header(), params=params,
        )
        while results:
            for result in results:
                yield result
                count += 1
                if max_records:
                    if count >= max_records:
                        print(f"{max_records} records exceeded, breaking.")
                        return
            params["offset"] += per_page
            results = self.call_api(
                endpoint=endpoint, method="GET", headers=self.send_json_header(), params=params,
            )
        return results

    def list_datasets(self, sort=None, max_records=None, offset=0, per_page=50):
        """
        Lists all Domo Datasets and appends them into a list of JSON data

        https://developer.domo.com/docs/dataset-api-reference/dataset#List%20DataSets

        :param sort: The DataSet field to sort by. Fields prefixed with a negative sign reverses the sort (i.e. '-name' does a reverse sort by the name of the DataSets)
        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 50 and the maximum is 50.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Dataset list
        """
        self.log.info(f"Listing datasets.")
        if per_page not in range(1, 51):
            raise ValueError("per_page must be between 1 and 50 (inclusive)")
        if sort not in [None, "cardCount", "name", "errorState", "lastUpdated"]:
            raise AirflowException(
                "Only the following fields can be used for sorting: cardCount, name, errorState, lastUpdated"
            )
        return self.handle_pagination(
            endpoint="v1/datasets?",
            sort=sort,
            max_records=max_records,
            offset=offset,
            per_page=per_page,
        )

    def retrieve_dataset(self, dataset_id):
        """
        Retrieves a Domo dataset. Includes the name, descriptions, columns, and owner of the dataset.

        https://developer.domo.com/docs/dataset-api-reference/dataset#Retrieve%20a%20DataSet

        :param dataset_id: The Domo dataset ID
        :return: Returns a DataSet object if valid DataSet ID was provided. When requesting, if the DataSet ID is related to a DataSet that has been deleted, 
                 a subset of the DataSet's information will be returned, including a deleted property, which will be true.
        """
        self.log.info(f"Retrieving dataset ID: {dataset_id}")
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}", method="GET", headers=self.send_json_header()
        )

    def retrieve_column_schema(self, dataset_id):
        """
        Retrieves a list of Domo column names and data types. This is pulled from the dataSet
        object and specifically filters for the ["schema"]["columns"] section
        
        [{'type': 'STRING', 'name': 'year'},
         {'type': 'STRING', 'name': 'month'}]
         
        :param dataset_id: Domo dataset ID
        :return: List of column names and type mapping
        """
        self.log.info(f"Retrieving column names and types for existing dataset: {dataset_id}.")
        r = self.call_api(
            endpoint=f"v1/datasets/{dataset_id}", method="GET", headers=self.receive_json_header()
        )
        r["schema"]["columns"] = [
            column
            for column in r["schema"]["columns"]
            if column["name"] not in ["_BATCH_ID_", "_BATCH_LAST_RUN_",]
        ]
        return r["schema"]["columns"]

    def retrieve_column_names(self, dataset_id):
        """
        Retrieves a list of Domo column names. This is pulled from the dataSet
        object and specifically filters for column names from the ["schema"]["columns"]
        section.
        
        ['year', 'month']
        
        :param dataset_id: Domo dataset ID
        :return: Domo dataset column names
        """
        self.log.info(f"Retrieving columns names for existing dataset: {dataset_id}.")
        r = self.call_api(
            endpoint=f"v1/datasets/{dataset_id}", method="GET", headers=self.receive_json_header()
        )
        columns = [
            column["name"]
            for column in r["schema"]["columns"]
            if column["name"] not in ["_BATCH_ID_", "_BATCH_LAST_RUN_",]
        ]
        return columns

    @staticmethod
    def create_dataset_object(name, description, column_schema):
        """Returns a dictionary in the expected dataset format for Domo"""
        body = {"name": name}
        body["description"] = description
        body["schema"] = {}
        body["schema"]["columns"] = column_schema
        return body

    def create_dataset(self, name, description, column_schema):
        """
        Creates a Domo DataSet. 

        https://developer.domo.com/docs/dataset-api-reference/dataset#Create%20a%20DataSet

        :param name: Dataset name
        :param description: Description of the dataset
        :param column_schema: A list of each column and data type mapping (STRING, DECIMAL, LONG, DOUBLE, DATE, and DATETIME)
                             [{"name": "column_name", "type": "LONG"}]
        :return: Dataset object
        """
        self.log.info(f"Creating a new Domo dataset.")
        body = self.create_dataset_object(name, description, column_schema)
        return self.call_api(
            endpoint=f"v1/datasets",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def update_dataset(self, dataset_id, name, description, column_schema):
        """
        Updates an existing Domo DataSet. 

        https://developer.domo.com/docs/dataset-api-reference/dataset#Update%20a%20DataSet

        :param dataset_id: ID of the dataset
        :param name: Dataset name
        :param description: Description of the dataset
        :param column_schema: A list of each column and data type mapping (STRING, DECIMAL, LONG, DOUBLE, DATE, and DATETIME)
                             [{"name": "column_name", "type": "LONG"}]
        :return: Dataset object
        """
        self.log.info(f"Updating dataset {dataset_id}")
        body = self.create_dataset_object(name, description, column_schema)
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}",
            method="PUT",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def query_dataset(self, dataset_id, query):
        """
        Queries a Domo dataset and returns the results. You can refer to the dataset as table and column names need
        to be surrounded by backticks.

        https://developer.domo.com/docs/dataset-api-reference/dataset#Query%20a%20DataSet
        
        query = "select * from table where `date` > '2020-01-01' limit 10;"
        
        :param dataset_id: The Domo dataset ID
        :param query: SQL query
        :return: None
        """
        self.log.info(f"Executing {query} against {dataset_id}.")
        body = {"sql": f"{query}"}
        return self.call_api(
            endpoint=f"v1/datasets/query/execute/{dataset_id}",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def export_dataset(self, dataset_id, include_header, file_path=None):
        """
        Exports the Domo dataset to a CSV file. Optionally, this can open the file to be read (by pandas, for example)

        https://developer.domo.com/docs/dataset-api-reference/dataset#Export%20data%20from%20DataSet

        :param dataset_id: Domo dataset ID
        :param file_path: Optional. Destination file path
        :param include_header: Boolean. Include table header.
        :return: CSV file
        """
        self.log.info(f"Exporting dataset: {dataset_id} to a file.")
        params = {"includeHeader": include_header}
        if file_path is not None:
            params["fileName"] = file_path
        response = self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/data",
            method="GET",
            headers=self.receive_csv_header(),
            params=params,
        )
        if file_path is not None:
            file_path = str(file_path)
            if not file_path.endswith(".csv"):
                file_path += ".csv"
            with open(file_path, "wb") as csv_file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        csv_file.write(chunk)
                self.log.info(f"Data exported to {file_path} successfully.")
        else:
            return bytes.decode(response.content)

    def upload_dataset(self, dataset_id, csv):
        """
        Import data into a DataSet in your Domo instance. This request will replace the data currently in the DataSet.
        
        https://developer.domo.com/docs/dataset-api-reference/dataset#Import%20data%20into%20DataSet
        
        From a file:
        with open(filepath, 'rb') as csv:
            hook.upload_dataset(dataset_id=id, csv=csv)

        From Azure Blob:
        with io.BytesIO() as input_io:
            blob_service.get_blob_to_stream(container_name=container, blob_name=filename, stream=input_io)
            hook.upload_dataset(dataset_id=id, csv=input_io.getvalue())

        :param dataset_id: The ID of the DataSet to have data imported
        :param csv: Chunk of rows in CSV format. 
        :return: None
        """
        self.log.info(f"Uploading data to {dataset_id}")
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/data",
            method="PUT",
            headers=self.send_csv_header(),
            data=csv,
        )

    def delete_dataset(self, dataset_id):
        """
        Permanently deletes a DataSet from your Domo instance. This can be done for all DataSets, not just those created through the API.
        
        https://developer.domo.com/docs/dataset-api-reference/dataset#Delete%20a%20DataSet
        
        :param dataset_id: The Domo dataset ID
        :return: None
        """
        self.call_api(
            endpoint=f"v1/datasets/{dataset_id}", method="DELETE", headers=self.send_json_header()
        )
        self.log.info(f"Deleted dataset: {dataset_id}")

    def list_streams(self, max_records=None, offset=0, per_page=500):
        """
        Lists all Domo Streams and appends them into a list of JSON data

        https://developer.domo.com/docs/streams-api-reference/streams#List%20Streams    

        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 500 and the maximum is 500.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Stream list
        """
        self.log.info(f"Listing streams.")
        if per_page not in range(1, 501):
            raise ValueError("per_page must be between 1 and 500 (inclusive)")
        return self.handle_pagination(
            endpoint="v1/streams?", max_records=max_records, offset=offset, per_page=per_page,
        )

    def search_streams(self, dataset_id=None, owner_id=None):
        """
        Retrieves a Domo Stream for a specific Dataset ID or Domo User ID

        https://developer.domo.com/docs/streams-api-reference/streams#Search%20Streams

        :params dataset_id: The Domo dataset ID
        :params owner_id: The Domo user ID of the dataset owner
        :return: None
        """
        if dataset_id is not None and owner_id is not None:
            raise AirflowException("You can only query for a dataset_id or an owner_id, not both.")
        if dataset_id is not None:
            self.log.info(f"Retrieving dataset {dataset_id}.")
            params = {"q": f"dataSource.id:{dataset_id}", "fields": "all"}
        elif owner_id is not None:
            self.log.info(f"Retrieving owner {dataset_id}.")
            params = {"q": f"dataSource.owner.id:{owner_id}", "fields": "all"}
        else:
            raise AirflowException("Either dataset_id or owner_id must be provided.")
        return self.call_api(
            endpoint=f"v1/streams/search?",
            method="GET",
            headers=self.send_json_header(),
            params=params,
        )

    def retrieve_stream(self, stream_id):
        """
        Retrieves the details of an existing Domo stream.

        https://developer.domo.com/docs/streams-api-reference/streams#Retrieve%20a%20Stream
        
        :param stream_id: The Domo stream ID
        :return: Returns a Stream object if valid Stream ID was provided. When requesting, if the Stream ID is related to a DataSet that has been deleted, 
                a subset of the Stream's information will be returned, including a deleted property, which will be true.
        """
        self.log.info(f"Retrieving stream ID: {stream_id}")
        return self.call_api(
            endpoint=f"v1/streams/{stream_id}", method="GET", headers=self.send_json_header()
        )

    def create_stream(self, name, description, column_schema, update_method):
        """
        Creates a Domo Stream. 
        
        https://developer.domo.com/docs/streams-api-reference/streams#Create%20a%20Stream
        
        :param name: Dataset name
        :param description: Description of the dataset
        :param column_schema: A list of each column and data type mapping (STRING, DECIMAL, LONG, DOUBLE, DATE, and DATETIME)
                             [{"name": "column_name", "type": "LONG"}]
        :param update_method: The data import behavior. Either "APPEND" or "REPLACE"
        :return: Dataset object
        """
        self.log.info(f"Creating a new Domo stream.")
        body = self.create_dataset_object(name, description, column_schema)
        body = {"dataSet": body}
        body["updateMethod"] = update_method.upper()
        return self.call_api(
            endpoint=f"v1/streams",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def create_execution(self, stream_id):
        """
        Creates a Domo execution for a Stream. This allows multiple data parts to be uploaded over time or in
        parallel before being committed and assembled again.

        https://developer.domo.com/docs/streams-api-reference/streams#Create%20a%20Stream%20execution

        :param stream_id: The Domo Stream ID
        :param update_method: REPLACE or APPEND
        :return: None
        """
        self.log.info(f"Creating execution for stream {stream_id}")
        return self.call_api(
            endpoint=f"v1/streams/{stream_id}/executions",
            method="POST",
            headers=self.send_json_header(),
        )

    def upload_part(self, stream_id, execution_id, part_id, csv, gzip=True):
        """
        Creates a data part within the Stream execution to upload chunks of rows to the DataSet. 
        The calling client should keep track of parts and order them accordingly in an increasing sequence. 
        If a part upload fails, retry the upload as all parts must be present before committing the stream execution.

        https://developer.domo.com/docs/streams-api-reference/streams#Upload%20a%20data%20part

        From a file:
        with open(filepath, 'rb') as csv:
            compressed_body = gzip.compress(csv.read())
            hook.upload_dataset(dataset_id=id, csv=compressed_body)

        From Azure Blob:
        with io.BytesIO() as input_io:
            blob_service.get_blob_to_stream(container_name=container, blob_name=filename, stream=input_io)
            compressed_body = gzip.compress(input_io.getvalue())
            hook.upload_dataset(dataset_id=id, csv=compressed_body)

        :param stream_id: The ID of the Stream of data being imported into a DataSet
        :param execution_id: The ID of the Stream execution within the Stream
        :param part_id: The ID of the data part being used to upload a subset of data within the Stream execution
        :param csv: Chunk of rows in CSV format. 
        :param gzip: Default true. Assumes the data is has been compressed before being sent.
        :return: None
        """
        self.log.info(f"Uploading to part {part_id}")
        if gzip:
            headers = self.send_gzip_header()
        else:
            headers = self.send_csv_header()
        return self.call_api(
            endpoint=f"v1/streams/{stream_id}/executions/{execution_id}/part/{part_id}",
            method="PUT",
            headers=headers,
            data=csv,
        )

    def commit_execution(self, stream_id, execution_id):
        """
        Finalizes the execution after all parts have been uploaded successfully.

        https://developer.domo.com/docs/streams-api-reference/streams#Commit%20a%20Stream%20execution

        :param stream_id: The Domo Stream ID
        :param execution_id: The ID of the Stream execution within the Stream
        :return: None
        """
        self.log.info(f"Committing execution {execution_id} for stream {stream_id}")
        return self.call_api(
            endpoint=f"v1/streams/{stream_id}/executions/{execution_id}/commit",
            method="PUT",
            headers=self.send_json_header(),
        )

    def list_executions(self, stream_id, max_records=None, offset=0, per_page=500):
        """
        Returns all Stream Execution objects that meet argument criteria from original request.

        https://developer.domo.com/docs/streams-api-reference/streams#List%20Stream%20executions

        :param stream_id: The ID of the stream
        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 500 and the maximum is 500.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Stream list
        """
        self.log.info(f"Listing stream executions for {stream_id}")
        if per_page not in range(1, 501):
            raise ValueError("per_page must be between 1 and 500 (inclusive)")
        return self.handle_pagination(
            endpoint="v1/streams/{stream_id}/executions",
            max_records=max_records,
            offset=offset,
            per_page=per_page,
        )

    def abort_execution(self, stream_id, execution_id):
        """
        Cancels the execution.

        https://developer.domo.com/docs/streams-api-reference/streams#Abort%20a%20Stream%20execution

        :param stream_id: The Domo Stream ID
        :param execution_id: The ID of the current execution
        :return: None
        """
        self.log.info(f"Aborting {execution_id} for stream {stream_id}.")
        return self.call_api(
            endpoint=f"v1/streams/{stream_id}/executions/{execution_id}/abort",
            method="PUT",
            headers=self.send_json_header(),
        )

    def update_stream_method(self, stream_id, update_method):
        """
        Changes the update method of a stream to REPLACE or APPEND, as specified

        https://developer.domo.com/docs/streams-api-reference/streams#Update%20a%20Stream

        :param stream_id: The Domo Stream ID
        :param update_method: REPLACE or APPEND
        :return: None
        """
        if update_method.upper() not in ["APPEND", "REPLACE"]:
            raise AirflowException("Update method must be APPEND or REPLACE")
        self.log.info(f"Changing updated method of stream {stream_id} to {update_method}.")
        body = {"updateMethod": update_method.upper()}
        return self.call_api(
            endpoint=f"v1/streams/{stream_id}",
            method="PATCH",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def delete_stream(self, stream_id):
        """
        Deletes a Domo stream. This does not delete the dataset.

        https://developer.domo.com/docs/streams-api-reference/streams#Delete%20a%20Stream
        
        :param stream_id: The Domo stream ID
        :return: None
        """
        self.call_api(
            endpoint=f"v1/streams/{stream_id}", method="DELETE", headers=self.send_json_header()
        )
        self.log.info(f"Deleted stream: {stream_id}.")

    def list_pdp(self, dataset_id):
        """
        List the Personalized Data Permission (PDP) policies for a specified DataSet.

        https://developer.domo.com/docs/dataset-api-reference/dataset#List%20Personalized%20Data%20Permission%20(PDP)%20policies

        :param dataset_id: The ID of the DataSet associated to the PDP policy
        :return: PDP policy list
        """
        self.log.info(f"Listing PDP policies for {dataset_id}.")
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/policies",
            method="GET",
            headers=self.send_json_header(),
        )

    def retrieve_pdp_policy(self, dataset_id, pdp_id):
        """Retrieve a policy from a DataSet within Domo. A DataSet is required for a PDP policy to exist.

        https://developer.domo.com/docs/dataset-api-reference/dataset#Retrieve%20a%20Personalized%20Data%20Permission%20(PDP)%20policy

        :param dataset_id: The Domo dataset ID
        :param pdp_id: The PDP ID
        :return: PDP Policy, a subset of the Dataset object
        """
        self.log.info(f"Retrieving PDP policy: {pdp_id} for {dataset_id}")
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/policies/{pdp_id}",
            method="GET",
            headers=self.receive_json_header(),
        )

    @staticmethod
    def create_pdp_object(
        name,
        filter_column=None,
        filter_not=None,
        filter_operator=None,
        filter_values=[],
        policy_type=None,
        users=[],
        groups=[],
    ):
        """Returns a dictionary in the expected PDP policy format for Domo"""
        if policy_type not in ["user", "system"]:
            raise AirflowException("Policy type must be either user or system")
        body = {"name": name}
        body["filters"] = []
        filter_body = {}
        filter_body["column"] = filter_column
        filter_body["not"] = filter_not
        filter_body["operator"] = filter_operator
        filter_body["values"] = filter_values
        body["filters"].append(filter_body)
        body["type"] = policy_type
        body["users"] = users
        body["groups"] = groups
        return body

    def create_pdp_policy(
        self,
        dataset_id,
        name,
        filter_column=None,
        filter_not=None,
        filter_operator=None,
        filter_values=[],
        policy_type=None,
        users=[],
        groups=[],
    ):
        """
        Creates a Domo PDP policy. 
        
        https://developer.domo.com/docs/dataset-api-reference/dataset#Create%20a%20Personalized%20Data%20Permission%20(PDP)%20policy
        
        :param dataset_id: The ID of the DataSet associated to the PDP policy
        :param name: Name of the PDP Policy
        :param filter_column: Name of the column to filter on
        :param filter_not: Determines if NOT is applied to the filter operation
        :param filter_operator: Matching operator (EQUALS)
        :param filter_values: Values to filter on
        :param policy_type: Type of policy (user or system)
        :param users: List of user IDs the policy applies to
        :param groups: List of group IDs the policy applies to
        :return: Subset of a Dataset object
        """
        self.log.info(f"Creating a new PDP policy for {dataset_id}.")
        body = self.create_pdp_object(
            name,
            filter_column,
            filter_not,
            filter_operator,
            filter_values,
            policy_type,
            users,
            groups,
        )
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/policies",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def update_pdp_policy(
        self,
        dataset_id,
        pdp_id,
        name,
        filter_column=None,
        filter_not=None,
        filter_operator=None,
        filter_values=[],
        policy_type=None,
        users=[],
        groups=[],
    ):
        """
        Update properties of an existing PDP Policy.

        https://developer.domo.com/docs/dataset-api-reference/dataset#Update%20a%20Personalized%20Data%20Permission%20(PDP)%20policy
        
        :param dataset_id: The ID of the DataSet associated to the PDP policy
        :param pdp_id: The ID the PDP policy
        :param name: Name of the PDP Policy
        :param filter_column: Name of the column to filter on
        :param filter_not: Determines if NOT is applied to the filter operation
        :param filter_operator: Matching operator (EQUALS)
        :param filter_values: Values to filter on
        :param policy_type: Type of policy (user or system)
        :param users: List of user IDs the policy applies to
        :param groups: List of group IDs the policy applies to
        :return: Subset of a Dataset object
        :return: None
        """
        self.log.info(f"Updating PDP Policy {pdp_id} for {dataset_id}")
        body = self.create_pdp_object(
            name,
            filter_column,
            filter_not,
            filter_operator,
            filter_values,
            policy_type,
            users,
            groups,
        )
        return self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/policies/{pdp_id}",
            method="PUT",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def delete_pdp(self, dataset_id, pdp_id):
        """
        Deletes a Domo PDP policy.
        :param dataset_id: The ID of the dataset associated to the PDP policy
        :param pdp_id: The Domo PDP Policy ID
        :return: None
        """
        self.call_api(
            endpoint=f"v1/datasets/{dataset_id}/policies/{pdp_id}",
            method="DELETE",
            headers=self.send_json_header(),
        )
        self.log.info(f"Deleted PDP ID {pdp_id} from dataset ID {dataset_id}.")

    def list_users(self, per_page=500, offset=0, max_records=None):
        """
        Lists all Domo Users and appends them into a list of JSON data

        https://developer.domo.com/docs/users-api-reference/users-2#List%20users

        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 500 and the maximum is 500.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: User list
        """
        self.log.info(f"Listing users.")
        return self.handle_pagination(
            endpoint="v1/users?", per_page=per_page, offset=offset, max_records=max_records
        )

    def retrieve_user(self, user_id):
        """
        Retrieves the details of an existing Domo user.

        https://developer.domo.com/docs/users-api-reference/users-2#Retrieve%20a%20user
        
        :param user_id: The Domo user ID
        :return: Returns a user object if valid user ID was provided. When requesting, if the user ID is related to a user that has been deleted, 
                 a subset of the user information will be returned, including a deleted property, which will be true.
        """
        self.log.info(f"Retrieving user ID: {user_id}")
        return self.call_api(
            endpoint=f"v1/users/{user_id}", method="GET", headers=self.send_json_header()
        )

    @staticmethod
    def create_user_object(
        name, employee_id, email, alternate_email, role, title, phone, location, time_zone, locale
    ):
        body = {"name": name}
        body["email"] = email.lower()
        body["role"] = role
        body["title"] = title
        body["alternateEmail"] = alternate_email
        body["phone"] = phone
        body["location"] = location
        body["timezone"] = time_zone
        body["locale"] = locale
        body["employeeId"] = employee_id
        return body

    def create_user(
        self,
        name,
        email,
        role,
        employee_id=None,
        alternate_email=None,
        title=None,
        phone=None,
        location=None,
        time_zone=None,
        locale=None,
        send_invite=False,
    ):
        """
        Creates a new user in your Domo instance.

        https://developer.domo.com/docs/users-api-reference/users-2#Create%20a%20user

        :param name: Required. User's full name
        :param email: Required. User's primary email used in profile
        :param role: Required. The role of the user created
        :param title: User's job title
        :param alternate_email: User's secondary email in profile
        :param phone: Primary phone number of user
        :param location: Free text that can be used to define office location
        :param time_zone: Time zone used to display to user the system times throughout Domo application
        :param locale: Locale used to display to user the system settings throughout Domo application
        :param employee_id: Employee ID within company. This replaces employee number
        :param send_invite: Send an email invitation to the user. Boolean.
        :return: User object
        """
        body = self.create_user_object(
            name=name,
            employee_id=employee_id,
            email=email,
            alternate_email=alternate_email,
            role=role,
            title=title,
            phone=phone,
            location=location,
            time_zone=time_zone,
            locale=locale,
        )
        params = {"sendInvite": send_invite}
        self.log.info(f"Creating a new Domo user: {body}")
        return self.call_api(
            endpoint=f"v1/users",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
            params=params,
        )

    def update_user(
        self,
        user_id,
        name,
        employee_id,
        email,
        alternate_email,
        role,
        title,
        phone,
        location,
        time_zone,
        locale,
    ):
        """
        Updates the specified user by providing values to parameters passed. 
        Any parameter left out of the request will cause the specific user’s attribute to remain unchanged.

        https://developer.domo.com/docs/users-api-reference/users-2#Update%20a%20user

        :param user_id: The Domo User ID
        :param name: User's full name
        :param email: User's primary email used in profile
        :param role: The role of the user created
        :param title: User's job title
        :param alternate_email: User's secondary email in profile
        :param phone: Primary phone number of user
        :param location: Free text that can be used to define office location
        :param time_zone: Time zone used to display to user the system times throughout Domo application
        :param locale: Locale used to display to user the system settings throughout Domo application
        :param employee_id: Employee ID within company. This replaces employee number
        :return: User object
        """
        body = self.create_user_object(
            name=name,
            employee_id=employee_id,
            email=email,
            alternate_email=alternate_email,
            role=role,
            title=title,
            phone=phone,
            location=location,
            time_zone=time_zone,
            locale=locale,
        )
        self.log.info(f"Updating {user_id} to {body}")
        return self.call_api(
            endpoint=f"v1/users/{user_id}",
            method="PUT",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def delete_user(self, user_id):
        """
        Permantently deletes a Domo User.

        https://developer.domo.com/docs/users-api-reference/users-2#Delete%20a%20user

        :param user_id: The Domo User ID
        :return: None
        """
        self.call_api(
            endpoint=f"v1/users/{user_id}", method="DELETE", headers=self.send_json_header()
        )
        self.log.info(f"Deleted user: {user_id}")

    def list_groups(self, per_page=500, offset=0, max_records=None):
        """
        Lists all Domo Groups and appends them into a list of JSON data

        https://developer.domo.com/docs/groups-api-reference/groups-2#List%20groups

        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 50 and the maximum is 50.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Group list
        """
        self.log.info(f"Listing groups.")
        return self.handle_pagination(
            endpoint="v1/groups?", per_page=per_page, offset=offset, max_records=max_records
        )

    def list_users_in_group(self, group_id, per_page=500, offset=0, max_records=None):
        """
        List the users in a group in your Domo instance.

        https://developer.domo.com/docs/groups-api-reference/groups-2#List%20users%20in%20a%20group

        :param max_records: The total number of records you would like to retrieve. 
                            If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 50 and the maximum is 50.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Group list
        """
        self.log.info(f"Listing groups.")
        return self.handle_pagination(
            endpoint=f"v1/groups/{group_id}/users",
            per_page=per_page,
            offset=offset,
            max_records=max_records,
        )

    def retrieve_group(self, group_id):
        """
        Retrieves the details of an existing Domo group.

        https://developer.domo.com/docs/groups-api-reference/groups-2#Retrieve%20a%20group
        
        :param group_id: The Domo group ID
        :return: Returns a group object if valid group ID was provided. When requesting, if the group ID is related to a customer that has been deleted, 
                 a subset of the group's information will be returned, including a deleted property, which will be true.
        """
        self.log.info(f"Retrieving group ID: {group_id}")
        return self.call_api(
            endpoint=f"v1/groups/{group_id}", method="GET", headers=self.send_json_header()
        )

    def create_group(self, name):
        """
        Creates a new group in your Domo instance.

        https://developer.domo.com/docs/groups-api-reference/groups-2#Create%20a%20group

        :param name: Required. The name of the group
        :return: Group object
        """
        self.log.info(f"Creating a new Domo group: {name}")
        body = {"name": name}
        return self.call_api(
            endpoint=f"v1/groups",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def update_group(self, group_id, name=None, active=True):
        """
        Updates the specified group by providing values to parameters passed. 
        Any parameter left out of the request will cause the specific group’s attribute to remain unchanged.

        https://developer.domo.com/docs/groups-api-reference/groups-2#Create%20a%20group

        :param group_id: The ID of the group.
        :param name: The name of the group
        :param active: Boolean. If the group is active
        :return: Group object
        """
        body = {"name": name}
        body["active"] = active
        self.log.info(f"Updating Domo group {group_id} to {body}")
        return self.call_api(
            endpoint=f"v1/groups",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def add_user_to_group(self, group_id, user_id):
        """
        Add user to a group in your Domo instance.

        https://developer.domo.com/docs/groups-api-reference/groups-2#Add%20a%20user%20to%20a%20group

        :param group_id: The ID of the group.
        :param user_id: The ID of the user being added
        :return: None
        """
        self.log.info(f"Add user {user_id} to group {group_id}")
        return self.call_api(
            endpoint=f"v1/groups/{group_id}/users/{user_id}",
            method="PUT",
            headers=self.send_json_header(),
        )

    def delete_user_from_group(self, group_id, user_id):
        """
        Remove a user from a group in your Domo instance.

        https://developer.domo.com/docs/groups-api-reference/groups-2#Remove%20a%20user%20from%20a%20group

        :param group_id: The Domo group ID
        :param user_id: The user ID being removed from the group
        :return: None
        """
        self.call_api(
            endpoint=f"v1/groups/{group_id}/users/{user_id}",
            method="DELETE",
            headers=self.send_json_header(),
        )
        self.log.info(f"Deleted user ID {user_id} from group {group_id}")

    def delete_group(self, group_id):
        """
        Permanently deletes a group from your Domo instance.

        https://developer.domo.com/docs/groups-api-reference/groups-2#Delete%20a%20group

        :param group_id: The Domo group ID
        :return: None
        """
        self.call_api(
            endpoint=f"v1/groups/{group_id}", method="DELETE", headers=self.send_json_header()
        )
        self.log.info(f"Deleted group ID: {group_id}")

    def list_pages(self, per_page=50, offset=0, max_records=None):
        """
        Lists all Domo Pages and appends them into a list of JSON data

        https://developer.domo.com/docs/page-api-reference/page#List%20pages    

        :param max_records: The total number of records you would like to retrieve. If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 50 and the maximum is 50.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Page list
        """
        self.log.info(f"Listing pages.")
        if per_page not in range(1, 51):
            raise ValueError("per_page must be between 1 and 50 (inclusive)")
        return self.handle_pagination(
            endpoint="v1/pages?", per_page=per_page, offset=offset, max_records=max_records
        )

    def retrieve_page(self, page_id):
        """
        Retrieves JSON data describing a specific Domo Page

        https://developer.domo.com/docs/page-api-reference/page#Retrieve%20a%20page

        :param page_id: The ID of the specific page being retrieved
        :return: JSON data for the Page
        """
        self.log.info(f"Retrieving page {page_id}")
        return self.call_api(
            endpoint=f"v1/pages/{page_id}", method="GET", headers=self.send_json_header(),
        )

    @staticmethod
    def create_page_object(
        name, parent_id, locked=False, card_ids=None, user_ids=None, group_ids=None
    ):
        body = {"name": name}
        body["parentId"] = parent_id
        body["locked"] = locked
        body["cardIds"] = card_ids
        body["visibility"] = {}
        body["visibility"]["userIds"] = user_ids
        body["visibility"]["groupIds"] = group_ids
        return body

    def create_page(self, name, parent_id, locked, card_ids, user_ids, group_ids):
        """
        Create a new Domo page.

        https://developer.domo.com/docs/page-api-reference/page#Create%20a%20page

        :param name: Will update the name of the page
        :param parentId: If provided, will either make the page a subpage or simply move the subpage
                                    to become a child of a different parent page. Integer
        :param locked: Will restrict access to edit page, Boolean
        :param collectionIds: Collections cannot be added or removed but the order can be updated based
                                         on the order of IDs provided in the argument
        :param cardIds: IDs provided will add or remove cards that are not a part of a page collection. Integer
        :param visibility: Shares pages with users or groups
        :param userIds: IDs provided will share page with associated users. Integer
        :param groupIds: IDs provided will share page with associated groups. Integer
        :return: Page object
        """
        body = self.create_page_object(
            name=name,
            parent_id=parent_id,
            locked=locked,
            card_ids=card_ids,
            user_ids=user_ids,
            group_ids=group_ids,
        )
        self.log.info(f"Creating new page: {body}")
        return self.call_api(
            endpoint=f"v1/pages",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def update_page(
        self, page_id, name, parent_id, locked, collection_ids, card_ids, user_ids, group_ids
    ):
        """
        Update the specified Page.

        https://developer.domo.com/docs/page-api-reference/page#Update%20a%20page

        :param page_id: The ID of the page being updated
        :param name: Will update the name of the page
        :param parentId: If provided, will either make the page a subpage or simply move the subpage
                                    to become a child of a different parent page. Integer
        :param locked: Will restrict access to edit page, Boolean
        :param collectionIds: Collections cannot be added or removed but the order can be updated based
                                         on the order of IDs provided in the argument
        :param cardIds: IDs provided will add or remove cards that are not a part of a page collection. Integer
        :param visibility: Shares pages with users or groups
        :param userIds: IDs provided will share page with associated users. Integer
        :param groupIds: IDs provided will share page with associated groups. Integer
        :return:
        """
        body = self.create_page_object(
            name=name,
            parent_id=parent_id,
            locked=locked,
            card_ids=card_ids,
            user_ids=user_ids,
            group_ids=group_ids,
        )
        body["collectionIds"] = collection_ids
        self.log.info(f"Updating {page_id} to {body}")
        return self.call_api(
            endpoint=f"v1/pages/{page_id}",
            method="PUT",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def delete_page(self, page_id):
        """
        Deletes a Domo page.

        https://developer.domo.com/docs/page-api-reference/page#Delete%20a%20page

        :param page_id: The Domo Page ID
        :return: None
        """
        self.call_api(
            endpoint=f"v1/pages/{page_id}", method="DELETE", headers=self.send_json_header()
        )
        self.log.info(f"Deleted page: {page_id}.")

    def retrieve_collection(self, page_id):
        """
        Retrieves JSON data describing a specific Domo Page

        https://developer.domo.com/docs/page-api-reference/page#Retrieve%20a%20page%20collection

        :param page_id: The ID of the specific page being retrieved
        :return: JSON data for the Collection
        """
        self.log.info(f"Retrieving collections for {page_id}")
        return self.call_api(
            endpoint=f"v1/pages/{page_id}/collections",
            method="GET",
            headers=self.send_json_header(),
        )

    @staticmethod
    def create_collection_object(title, description, card_ids):
        body = {"title": title}
        body["description"] = description
        body["cardIds"] = card_ids
        return body

    def create_collection(self, page_id, title, description, card_ids):
        """
        Create a new Domo card collection.

        https://developer.domo.com/docs/page-api-reference/page#Create%20a%20page%20collection

        :param page_id: ID of the page where the Collection belongs
        :param title: Page collection's name displayed above the set of cards
        :param description: Additional text within the page collection
        :param cardIds: List of card IDs provided will add or remove cards that are not a part of a page collection
        :return:
        """
        body = self.create_collection_object(title, description, card_ids)
        self.log.info(f"Creating a new Domo collection for page {page_id}: {body}")
        return self.call_api(
            endpoint=f"v1/pages/{page_id}/collections",
            method="POST",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def update_collection(self, page_id, collection_id, title, description, card_ids):
        """
        Create a new Domo card collection.

        https://developer.domo.com/docs/page-api-reference/page#Update%20a%20page%20collection

        :param page_id: ID of the page where the Collection belongs
        :param collection_id: ID of page collection that needs to be updated
        :param title: Page collection's name displayed above the set of cards
        :param description: Additional text within the page collection
        :param cardIds: List of card IDs provided will add or remove cards that are not a part of a page collection
        :return:
        """
        body = self.create_collection_object(title, description, card_ids)
        self.log.info(f"Updating collection ID {collection_id} for page {page_id}: {body}")
        return self.call_api(
            endpoint=f"v1/pages/{page_id}/collections/{collection_id}",
            method="PUT",
            headers=self.send_json_header(),
            data=json.dumps(body, default=str),
        )

    def delete_collection(self, page_id, collection_id):
        """
        Permanently deletes a page collection from your Domo instance. You can use the retrieve_collection
        method to list the collection_ids for a page.

        https://developer.domo.com/docs/page-api-reference/page#Delete%20a%20page%20collection

        :param page_id: The ID of the page where the collection is
        :param collection_id: The ID of the page collection to delete. String
        :return: None
        """
        self.call_api(
            endpoint=f"v1/pages/{page_id}/collections/{collection_id}",
            method="DELETE",
            headers=self.send_json_header(),
        )
        self.log.info(f"Deleted collection {collection_id} from page {page_id}.")

    def list_activity_logs(
        self, start_time, end_time=None, user=None, max_records=None, offset=0, per_page=1000
    ):
        """
        Retrieves activity logs from Domo from the start time to an optional end time. The start time accepts
        the unix timestamp in milliseconds, or a string or datetime which will be converted to unix time.

        https://developer.domo.com/docs/activity-log-api-reference/activity-log

        :param start_time: The start time (milliseconds) of when you want to receive log events. This can also be a string timestamp.
        :param end_time: The end time (milliseconds) of when you want to receive log events. This can also be a string timestamp.
        :param user: Optional Domo user ID that you would like to pull records for
        :param max_records: The total number of records you would like to retrieve. If this value is less than the per_page value, then the limit will be used instead
        :param per_page: The amount of records you want to retrieve. The default is 1000 and the maximum is 1000.
        :param offset: The offset location of records you retrieve. Default is 0.
        :return: Activity logs
        """
        if isinstance(start_time, int):
            start_time = start_time
        else:
            start_time = pendulum.parse(start_time).int_timestamp * 1000
        if end_time is not None:
            if isinstance(end_time, int):
                end_time = end_time
            else:
                end_time = pendulum.parse(end_time).int_timestamp
        if per_page not in range(1, 1001):
            raise ValueError("per_page must be between 1 and 1000 (inclusive)")
        self.log.info(f"Retrieving activity logs since {start_time}")
        return self.handle_pagination(
            endpoint="v1/audit?",
            start_time=start_time,
            end_time=end_time,
            user=user,
            max_records=max_records,
            offset=offset,
            per_page=per_page,
        )
