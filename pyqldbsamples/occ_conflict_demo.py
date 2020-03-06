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

from botocore.exceptions import ClientError

from pyqldb.errors import is_occ_conflict_exception
from pyqldbsamples.model.sample_data import SampleData, convert_object_to_ion
from pyqldbsamples.constants import Constants
from pyqldbsamples.connect_to_ledger import create_qldb_session

logger = getLogger(__name__)
basicConfig(level=INFO)


def execute_transaction(qldb_session, transaction, statement, parameter):
    """
    Execute statement with parameters. If it was unsuccessful, retry with a new transaction.

    :type qldb_session: :py:class:`pyqldb.session.qldb_session.QldbSession`
    :param qldb_session: An instance of the QldbSession class.

    :type transaction: :py:class:`pyqldb.transaction.transaction.Transaction`
    :param transaction: An open transaction.

    :type statement: str
    :param statement: The query to execute.

    :type parameter: :py:class:`amazon.ion.simple_types.IonPyValue`
    :param parameter: The Ion value or Python native type that is convertible to Ion for filling in parameters of the
                      statement.
    """
    for i in range(Constants.RETRY_LIMIT):
        try:
            transaction.execute_statement(statement, parameter)
            logger.info('Execute successful after {} retries.'.format(i))
            break
        except ClientError as ce:
            if is_occ_conflict_exception(ce):
                logger.error('Execute on QLDB session failed due to an OCC conflict. Restart transaction.')
                transaction = qldb_session.start_transaction()


def commit_transaction(qldb_session, transaction, statement, parameter):
    """
    Commit the transaction and retry up to a constant number of times.

    :type qldb_session: :py:class:`pyqldb.session.qldb_session.QldbSession`
    :param qldb_session: An instance of the QldbSession class.

    :type transaction: :py:class:`pyqldb.transaction.transaction.Transaction`
    :param transaction: An instance of the Transaction class.

    :type statement: str
    :param statement: The query to execute.

    :type parameter: :py:class:`amazon.ion.simple_types.IonPyValue`
    :param parameter: The Ion value or Python native type that is convertible to Ion for filling in parameters of the
                      statement.
    """
    for i in range(Constants.RETRY_LIMIT):
        try:
            transaction.commit()
            logger.info('Commit successful after {} retries.'.format(i))
            break
        except ClientError as ce:
            if is_occ_conflict_exception(ce):
                logger.error('Commit failed due to an OCC conflict. Restart transaction.')
                transaction = qldb_session.start_transaction()
                execute_transaction(qldb_session, transaction, statement, parameter)


if __name__ == '__main__':
    """
    Demonstrates how to handle OCC conflicts, where two users try to execute and commit changes to the same document.
    When OCC conflict occurs on execute or commit, implicitly handled by restarting the transaction.
    In this example, two sessions on the same ledger try to access the registration city for the same Vehicle Id.
    """
    vehicle_vin = SampleData.VEHICLE_REGISTRATION[0]['VIN']
    parameters = convert_object_to_ion(vehicle_vin)
    query1 = "UPDATE VehicleRegistration AS v SET v.City = 'Tukwila' WHERE v.VIN = ?"
    query2 = 'SELECT City FROM VehicleRegistration AS v WHERE v.VIN = ?'

    with create_qldb_session() as session1, create_qldb_session() as session2:
        logger.info('Updating the VehicleRegistration city in transaction 1...')
        transaction1 = session1.start_transaction()
        logger.info('Selecting the VehicleRegistration city in transaction 2...')
        transaction2 = session2.start_transaction()

        logger.info('Executing transaction 1')
        execute_transaction(session1, transaction1, query1, parameters)
        logger.info('Executing transaction 2')
        execute_transaction(session2, transaction2, query2, parameters)

        logger.info('Committing transaction 1...')
        commit_transaction(session1, transaction1, query1, parameters)

        # The first attempt to commit on transaction 2 will fail due to an OCC conflict.
        logger.info('Committing transaction 2...')
        commit_transaction(session2, transaction2, query2, parameters)
