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

from pyqldbsamples.connect_to_ledger import create_qldb_driver
from pyqldbsamples.constants import Constants

logger = getLogger(__name__)
basicConfig(level=INFO)


def list_tables(ledger_name):
    """
    List all tables.

    :type ledger_name: str
    :param ledger_name: The name of the ledger.

    :rtype: list
    :return: List of tables.
    """
    logger.info("Let's list all the tables...")
    with create_qldb_driver(ledger_name) as driver:
        logger.info("Success. List of tables:")
        tables = driver.list_tables()
        for table in tables:
            logger.info(table)
    return tables


def main(ledger_name=Constants.LEDGER_NAME):
    """
    List all the tables in the configured ledger in QLDB.
    """
    try:
        list_tables(ledger_name)
    except Exception as e:
        logger.exception('Unable to list tables!')
        raise e


if __name__ == '__main__':
    main()
