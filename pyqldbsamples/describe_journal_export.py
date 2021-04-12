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
from logging import basicConfig, getLogger, INFO
from sys import argv

from boto3 import client

from pyqldbsamples.constants import Constants

logger = getLogger(__name__)
basicConfig(level=INFO)
qldb_client = client('qldb')


def describe_journal_export(ledger_name, export_id):
    """
    Describe a journal export.

    :type ledger_name: str
    :param ledger_name: The ledger from which the journal is being exported.

    :type export_id: str
    :param export_id: The ExportId of the journal.

    :rtype: dict
    :return: Result from the request.
    """
    logger.info("Let's describe a journal export for ledger with name: {}, exportId: {}.".format(ledger_name,
                                                                                                 export_id))
    export_result = qldb_client.describe_journal_s3_export(Name=ledger_name, ExportId=export_id)
    logger.info('Export described. Result = {}.'.format(export_result['ExportDescription']))
    return export_result


def main():
    """
    Describe a specific journal export with the given ExportId.
    """
    if len(argv) != 2:
        raise ValueError('Missing ExportId argument in DescribeJournalExport')
    export_id = argv[1]

    logger.info('Running describe export journal tutorial with ExportId: {}.'.format(export_id))

    try:
        describe_journal_export(Constants.LEDGER_NAME, export_id)
    except Exception as e:
        logger.exception('Unable to describe an export!')
        raise e


if __name__ == '__main__':
    main()
