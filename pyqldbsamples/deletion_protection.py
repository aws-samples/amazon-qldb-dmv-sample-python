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

from pyqldbsamples.create_ledger import wait_for_active
from pyqldbsamples.delete_ledger import delete_ledger, set_deletion_protection

logger = getLogger(__name__)
basicConfig(level=INFO)
qldb_client = client('qldb')

LEDGER_NAME = 'deletion-protection-demo'


def create_with_deletion_protection(ledger_name):
    """
    Create a new ledger with the specified name and with deletion protection enabled.

    :type ledger_name: str
    :param ledger_name: Name for the ledger to be created.

    :rtype: dict
    :return: Result from the request.
    """
    logger.info("Let's create the ledger with name: {}...".format(ledger_name))
    result = qldb_client.create_ledger(Name=ledger_name, PermissionsMode='ALLOW_ALL')
    logger.info('Success. Ledger state: {}'.format(result.get('State')))
    return result


if __name__ == '__main__':
    """
    Demonstrate the protection of QLDB ledgers against deletion.
    """
    try:
        create_with_deletion_protection(LEDGER_NAME)
        wait_for_active(LEDGER_NAME)
        try:
            delete_ledger(LEDGER_NAME)
        except qldb_client.exceptions.ResourcePreconditionNotMetException:
            logger.info('Ledger protected against deletions! Turning off deletion protection now.')
        set_deletion_protection(LEDGER_NAME, False)
        delete_ledger(LEDGER_NAME)
    except Exception:
        logger.exception('Error while updating or deleting the ledger!')
