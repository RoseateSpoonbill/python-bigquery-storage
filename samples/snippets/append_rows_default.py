# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START bigquerystorage_append_rows_default]
"""
Description: This code sample demonstrates how to write records in default mode
using the low-level generated client for Python.

Documentation to reference
    * https://cloud.google.com/python/docs/reference/bigquerystorage/latest
    * https://googleapis.dev/python/protobuf/latest/index.html

Information about the _default stream:
    * name format: projects/((project_id))/datasets/{{dataset_id}}/tables/{{table_id}}/_default
    * create_write_stream
        * Every table has a special stream named '_default' to which data can be written
        * This stream doesn't need to be created using CreateWriteStream
        * It is a stream that can be used simultaneously by any number of clients
        * Data written to this stream is considered committed as soon as an acknowledgement is received.
    * append_rows
        * No offset information will be returned for appends to a default stream.
        * For COMMITTED streams (which includes the default stream), data is visible immediately upon successful append.
    * finalize_write_stream
        * Finalize is not supported on the '_default' stream.
    * flush_rows
        * Flush is not supported on the _default stream, since it is not BUFFERED.

If you update the protocol buffer definition (i.e. the customer_record.proto file),
you need to regenerate the customer_record_pb2.py module by running this command
from the samples/snippets directory to generate the customer_record_pb2.py module:
    protoc --python_out=. customer_record.proto

"""

from xmlrpc.client import boolean
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2
import logging
import os
import sys
from googleapiclient.discovery import build
import httplib2

from . import customer_record_pb2

# Figure out if we defined the log level when we called this file (` --log={{LOGLEVEL}}`)
logging_level = os.environ.get('LOGLEVEL', 'WARNING').upper()
logging_numeric_level = getattr(logging, logging_level, None)
if not isinstance(logging_numeric_level, int):
    raise ValueError('Invalid log level: %s' % logging_numeric_level)

# Configure the logging
logging.basicConfig(
    level=logging_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        #logging.FileHandler("debug.log"),  # Uncomment if you want to log the output to a file (how the file is currently set up, it doesn't log everything)
        logging.StreamHandler()
        #logging.NullHandler()
    ]
)

if logging_level == logging.DEBUG:
    # enable logging of all HTTP request and response headers and bodies
    httplib2.debuglevel = 4

    # print out arguments passed to this file
    logging.debug(sys.argv)


def create_row_data(row_num: int, name: str):
    row = customer_record_pb2.CustomerRecord()
    row.row_num = row_num
    row.customer_name = name
    return row.SerializeToString()


def append_rows_default(project_id: str, dataset_id: str, table_id: str):

    """Create a write stream, write some sample data, and commit the stream."""
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, table_id)
    stream_name = f'{parent}/_default'

    # Create a template with fields needed for the first request.
    request_template = types.AppendRowsRequest()

    # The initial request must contain the stream name.
    request_template.write_stream = stream_name

    # So that BigQuery knows how to parse the serialized_rows, generate a
    # protocol buffer representation of your message descriptor.
    proto_schema = types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    customer_record_pb2.CustomerRecord.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data

    # Some stream types support an unbounded number of requests. Construct an
    # AppendRowsStream to send an arbitrary number of requests to a stream.
    append_rows_stream = writer.AppendRowsStream(write_client, request_template)

    # Create a batch of row data by appending proto2 serialized bytes to the
    # serialized_rows repeated field.
    proto_rows = types.ProtoRows()
    proto_rows.serialized_rows.append(create_row_data(7, "Luisa"))
    proto_rows.serialized_rows.append(create_row_data(8, "Alex"))
    
    # Keep track of which requests the server has acknowledged and resume the
    # stream at the first non-acknowledged message. If the server has already
    # processed a message with that offset, it will return an ALREADY_EXISTS
    # error, which can be safely ignored.

    request = types.AppendRowsRequest()
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows
    request.proto_rows = proto_data

    # Send an append rows request to the open stream
    append_rows_stream.send(request)
    
    # Shutdown background threads and close the streaming connection.
    # Note: If you turn on DEBUG logging, you will get a 499 error that 
        # the grpc._channel._MultiThreadedRendezvous terminated and was 
        # "Locally cancelled by application!"  You can ignore that message
    append_rows_stream.close()

    print(f"Writes to stream: '{stream_name}' have been committed.")
# [END bigquerystorage_append_rows_default]
