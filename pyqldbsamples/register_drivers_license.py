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
from datetime import datetime

from pyqldbsamples.model.sample_data import convert_object_to_ion, get_document_ids
from pyqldbsamples.constants import Constants
from pyqldbsamples.insert_document import insert_documents
from pyqldbsamples.connect_to_ledger import create_qldb_session

logger = getLogger(__name__)
basicConfig(level=INFO)


def person_already_exists(transaction_executor, gov_id):
    """
    Verify whether a driver already exists in the database.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type gov_id: str
    :param gov_id: The government ID to search `Person` table against.

    :rtype: bool
    :return: If the Person has been registered.
    """
    query = 'SELECT * FROM Person AS p WHERE p.GovId = ?'
    cursor = transaction_executor.execute_statement(query, convert_object_to_ion(gov_id))
    try:
        next(cursor)
        return True
    except StopIteration:
        return False


def person_has_drivers_license(transaction_executor, document_id):
    """
    Check if the driver already has a driver's license using their unique document ID.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type document_id: str
    :param document_id: The document ID to check.

    :rtype: bool
    :return: If the Person has a drivers license.
    """
    cursor = lookup_drivers_license_for_person(transaction_executor, document_id)
    try:
        next(cursor)
        return True
    except StopIteration:
        return False


def lookup_drivers_license_for_person(transaction_executor, person_id):
    """
    Query drivers license table by person ID.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type person_id: str
    :param person_id: The person ID to check.

    :rtype: :py:class:`pyqldb.cursor.stream_cursor.StreamCursor`
    :return: Cursor on the result set of a statement query.
    """
    query = 'SELECT * FROM DriversLicense AS d WHERE d.PersonId = ?'
    cursor = transaction_executor.execute_statement(query, person_id)
    return cursor


def register_new_driver(transaction_executor, driver):
    """
    Register a new driver in QLDB if not already registered.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type driver: dict
    :param driver: The driver's license to register.
    """
    gov_id = driver['GovId']
    if person_already_exists(transaction_executor, gov_id):
        logger.info('Person with this GovId already exists.')
        result = next(get_document_ids(transaction_executor, Constants.PERSON_TABLE_NAME, 'GovId', gov_id))
    else:
        result = insert_documents(transaction_executor, Constants.PERSON_TABLE_NAME, [driver])
        result = result[0]
    return result


def register_new_drivers_license(transaction_executor, driver, new_license):
    """
    Register a new driver and a new driver's license in a single transaction.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type driver: dict
    :param driver: The driver's license to register.

    :type new_license: dict
    :param new_license: The driver's license to register.
    """
    person_id = register_new_driver(transaction_executor, driver)
    if person_has_drivers_license(transaction_executor, person_id):
        gov_id = driver['GovId']
        logger.info("Person with government ID '{}' already has a license! No new license added.".format(gov_id))
    else:
        logger.info("Registering new driver's license...")
        # Update the new license with new driver's unique PersonId.
        new_license.update({'PersonId': str(person_id)})
        statement = 'INSERT INTO DriversLicense ?'
        transaction_executor.execute_statement(statement, convert_object_to_ion(new_license))

        cursor = lookup_drivers_license_for_person(transaction_executor, person_id)
        try:
            next(cursor)
            logger.info('Successfully registered new driver.')
            return
        except StopIteration:
            logger.info('Problem occurred while inserting new license, please review the results.')
            return


if __name__ == '__main__':
    """
    Register a new driver's license.
    """
    try:
        with create_qldb_session() as session:
            new_driver = {
                'FirstName': 'Kate',
                'LastName': 'Mulberry',
                'Address': '22 Commercial Drive, Blaine, WA, 97722',
                'DOB': datetime(1995, 2, 9),
                'GovId': 'AQQ17B2342',
                'GovIdType': 'Passport'
            }
            drivers_license = {
                'PersonId': '',
                'LicenseNumber': '112 360 PXJ',
                'LicenseType': 'Full',
                'ValidFromDate': datetime(2018, 6, 30),
                'ValidToDate': datetime(2022, 10, 30)
            }
            session.execute_lambda(lambda executor: register_new_drivers_license(executor, new_driver, drivers_license),
                                   retry_indicator=lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))
    except Exception:
        logger.exception('Error registering new driver.')
