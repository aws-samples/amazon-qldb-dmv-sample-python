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

from boto3 import client

from pyqldbsamples.constants import Constants

logger = getLogger(__name__)
basicConfig(level=INFO)
MAX_RESULTS = 2
qldb_client = client('qldb')


def list_all_journal_exports():
    """
    List all journal exports.
    """
    logger.info("Let's list journal exports for the AWS account.")

    next_token = ''
    list_of_results = []
    while next_token is not None:
        if next_token == '':
            result = qldb_client.list_journal_s3_exports(MaxResults=MAX_RESULTS)
        else:
            result = qldb_client.list_journal_s3_exports(MaxResults=MAX_RESULTS, NextToken=next_token)
        next_token = result.get('NextToken')
        if result.get('JournalS3Exports') != []:
            list_of_results.append(result.get('JournalS3Exports'))
    logger.info('Success. List of journal exports: {}.'.format(list_of_results))


def list_journal_export_with_ledger_name(ledger_name):
    """
    List all journal exports for the given ledger.

    :type ledger_name: str
    :param ledger_name: Name of the ledger to list journal exports for.
    """
    logger.info("Let's list journal exports for the ledger with name: {}...".format(ledger_name))

    next_token = ''
    list_of_results = []
    while next_token is not None:
        if next_token == '':
            result = qldb_client.list_journal_s3_exports_for_ledger(Name=ledger_name, MaxResults=MAX_RESULTS)
        else:
            result = qldb_client.list_journal_s3_exports_for_ledger(Name=ledger_name, MaxResults=MAX_RESULTS,
                                                                    NextToken=next_token)
        next_token = result.get('NextToken')
        if result.get('JournalS3Exports') != []:
            list_of_results.append(result.get('JournalS3Exports'))
    logger.info('Success. List of journal exports: {}.'.format(list_of_results))


def main(ledger_name=Constants.LEDGER_NAME):
    """
    List the journal exports of a given QLDB ledger.
    """
    try:
        list_journal_export_with_ledger_name(ledger_name)
    except Exception as e:
        logger.exception('Unable to list exports!')
        raise e


if __name__ == '__main__':
    main()
