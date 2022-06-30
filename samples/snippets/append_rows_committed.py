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

# [START bigquerystorage_append_rows_committed]
"""
Description: This code sample demonstrates how to write records in committed mode
using the low-level generated client for Python.

Documentation to reference
    * https://cloud.google.com/python/docs/reference/bigquerystorage/latest
    * https://googleapis.dev/python/protobuf/latest/index.html

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


def append_rows_committed(project_id: str, dataset_id: str, table_id: str):

    """Create a write stream, write some sample data, and commit the stream."""
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, table_id)
    write_stream = types.WriteStream()

    # When creating the stream, choose the type. Use the COMMITTED type to automatically
    # commit the stream and for data to appear as soon as the write is acknowledged. See:
    # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
    write_stream.type_ = types.WriteStream.Type.COMMITTED
    write_stream = write_client.create_write_stream(
        parent=parent, write_stream=write_stream
    )
    stream_name = write_stream.name

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
    proto_rows.serialized_rows.append(create_row_data(4, "Ming"))
    proto_rows.serialized_rows.append(create_row_data(5, "Vinay"))
    
    # Set an offset to allow resuming this stream if the connection breaks.
    # Keep track of which requests the server has acknowledged and resume the
    # stream at the first non-acknowledged message. If the server has already
    # processed a message with that offset, it will return an ALREADY_EXISTS
    # error, which can be safely ignored.
    #
    # The first request must always have an offset of 0.
    request = types.AppendRowsRequest()
    request.offset = 0
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows
    request.proto_rows = proto_data

    # Send an append rows request to the open stream
    response_future_1 = append_rows_stream.send(request)

    # Send another batch.
    proto_rows = types.ProtoRows()
    proto_rows.serialized_rows.append(create_row_data(6, "Roberto"))

    # Since this is the second request, you only need to include the row data.
    # The name of the stream and protocol buffers DESCRIPTOR is only needed in
    # the first request.
    request = types.AppendRowsRequest()
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows
    request.proto_rows = proto_data

    # Offset must equal the number of rows that were previously sent.
    request.offset = 2

    response_future_2 = append_rows_stream.send(request)

    print(response_future_1.result())
    print(response_future_2.result())
    
    # Shutdown background threads and close the streaming connection.
    # Note: If you turn on DEBUG logging, you will get a 499 error that 
        # the grpc._channel._MultiThreadedRendezvous terminated and was 
        # "Locally cancelled by application!"  You can ignore that message
    append_rows_stream.close()

    print(f"Writes to stream: '{write_stream.name}' have been committed.")
# [END bigquerystorage_append_rows_committed]
