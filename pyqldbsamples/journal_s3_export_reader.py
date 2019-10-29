# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# This code expects that you have AWS credentials setup per:
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
from json import dumps as json_dumps
from logging import getLogger

from amazon.ion.simpleion import loads

from pyqldbsamples.qldb.journal_block import from_ion

logger = getLogger(__name__)


def compare_key_with_content_range(file_key, first_block, last_block):
    """
    Compare the expected block range, derived from File Key, with the actual object content.

    :type file_key: str
    :param file_key: The key of data file containing the chunk of journal block. The fileKey pattern is
                     {[strandId].[firstSequenceNo]-[lastSequenceNo].ion}.

    :type first_block: :py:class:`pyqldbsamples.qldb.journal_block.JournalBlock`
    :param first_block: The first block in the block chain for a particular journal strand.

    :type last_block: :py:class:`pyqldbsamples.qldb.journal_block.JournalBlock`
    :param last_block: The last block in the block chain for a particular journal strand.

    :raises RuntimeError: If the SequenceNo on the blockAddress does not match the expected SequenceNo.
    """
    sequence_no_range = file_key.split(".")[1]
    key_tokens = sequence_no_range.split("-")
    start_sequence_no = key_tokens[0]
    last_sequence_no = key_tokens[1]

    if str(first_block.block_address.get("sequenceNo")) != start_sequence_no:
        raise RuntimeError('Expected first block SequenceNo to be {}.'.format(start_sequence_no))
    if str(last_block.block_address.get("sequenceNo")) != last_sequence_no:
        raise RuntimeError('Expected last block SequenceNo to be {}.'.format(last_sequence_no))


def filter_for_initial_manifest(objects, manifest):
    """
    Find the initial manifest created at the beginning of a export request.

    :type objects: list
    :param objects: List of objects in a particular bucket.

    :type manifest: str
    :param manifest: The expected identifier for the initial manifest.

    :rtype: str
    :return: The identifier for the initial manifest object.

    :raises RuntimeError: If the initial manifest is not found.
    """
    for obj in objects:
        key = obj['Key'].casefold()
        if key == manifest.casefold():
            return key
    raise RuntimeError('Initial manifest not found.')


def filter_for_completed_manifest(objects):
    """
    Find the final manifest objects created after the completion of an export job.

    :type objects: list
    :param objects: List of objects in a particular bucket.

    :rtype: str
    :return: The identifier for the final manifest object.

    :raises RuntimeError: If the final manifest is not found.
    """
    for obj in objects:
        key = obj['Key']
        if key.casefold().endswith("completed.manifest"):
            return key
    raise RuntimeError('Completed manifest not found.')


def get_data_file_keys_from_manifest(manifest_object):
    """
    Retrieve the ordered list of data object keys within the given final manifest.

    :type manifest_object: str
    :param manifest_object: The content of the final manifest.

    :rtype: list
    :return: List of data object keys.
    """
    ion_keys = loads(manifest_object).get('keys')
    list_of_keys = list(ion_keys)
    return list_of_keys


def get_journal_blocks(s3_object):
    """
    Parse the given S3 object's content for the journal data objects in Ion format.

    :type s3_object: str
    :param s3_object: The content within a S3 object.

    :rtype: list
    :return: List of journal blocks.

    :raises RuntimeError: If there is an error loading the journal.
    """
    journals = s3_object.split('} {')
    journal_blocks = []

    for i in range(len(journals)):
        if i == 0:
            journals[i] = journals[i] + "}"
        elif i == len(journals)-1:
            journals[i] = "{" + journals[i]
        else:
            journals[i] = "{" + journals[i] + "}"
        try:
            parsed_journal = from_ion(loads(journals[i]))
            journal_blocks.append(parsed_journal)
        except ValueError as ve:
            raise RuntimeError('Failed to load journal: {}'.format(ve))
    logger.info('Found {} block(s).'.format(len(journal_blocks)))
    return journal_blocks


def read_export(describe_journal_export_result, s3_client):
    """
    Read the S3 export within a journal block.

    :type describe_journal_export_result: dict
    :param describe_journal_export_result: The result from QLDB describing a journal export.

    :type s3_client: :py:class:`botocore.client.BaseClient`
    :param s3_client: The low-level S3 client.

    :rtype: list
    :return: List of journal blocks.
    """
    export_configuration = describe_journal_export_result.get('S3ExportConfiguration')
    prefix = export_configuration.get('Prefix')
    bucket_name = export_configuration.get('Bucket')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response['Contents']
    logger.info('Found the following objects for list from S3:')
    for obj in objects:
        logger.info(obj['Key'])

    # Validate initial manifest file was written
    expected_manifest_key = "{}{}.started.manifest".format(prefix, describe_journal_export_result.get('ExportId'))
    initial_manifest = filter_for_initial_manifest(objects, expected_manifest_key)
    logger.info('Found the initial manifest with key: {}.'.format(initial_manifest))

    # Find the final manifest file, it should contain the exportId in it.
    completed_manifest_file_key = filter_for_completed_manifest(objects)
    completed_manifest_object = s3_client.get_object(Bucket=bucket_name, Key=completed_manifest_file_key)['Body']\
        .read().decode('utf-8')

    data_file_keys = get_data_file_keys_from_manifest(completed_manifest_object)

    logger.info('Found the following keys in the manifest files: {}.'.format(json_dumps(data_file_keys, indent=4)))
    journal_blocks = []
    for key in data_file_keys:
        logger.info('Reading file with S3 key {} from bucket: {}.'.format(key, bucket_name))
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)['Body'].read().decode('utf-8')
        blocks = get_journal_blocks(s3_object)
        compare_key_with_content_range(key, blocks[0], blocks[len(blocks) - 1])
        journal_blocks.append(blocks)
    return journal_blocks
