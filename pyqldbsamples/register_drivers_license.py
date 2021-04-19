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
from pyqldbsamples.connect_to_ledger import create_qldb_driver

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


def register_new_person(driver, person):
    """
    Register a new person in QLDB if not already registered.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type person: dict
    :param person: The person to register.

    :rtype: str
    :return: The person ID.
    """
    gov_id = person['GovId']
    if driver.execute_lambda(lambda executor: person_already_exists(executor, gov_id)):
        logger.info('Person with this GovId already exists.')

        result = driver.execute_lambda(lambda executor: get_document_ids(executor, Constants.PERSON_TABLE_NAME,
                                                                         'GovId', gov_id))
        result = result[0]
    else:
        result = insert_documents(driver, Constants.PERSON_TABLE_NAME, [person])
        result = result[0]
    return result


def register_new_drivers_license(driver, person, new_license):
    """
    Register a new person and a new driver's license.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type person: dict
    :param person: The person to register.

    :type new_license: dict
    :param new_license: The driver's license to register.
    """
    person_id = register_new_person(driver, person)
    if driver.execute_lambda(lambda executor: person_has_drivers_license(executor, person_id)):
        gov_id = person['GovId']
        logger.info("Person with government ID '{}' already has a license! No new license added.".format(gov_id))
    else:
        logger.info("Registering new driver's license...")
        # Update the new license with new driver's unique PersonId.
        new_license.update({'PersonId': str(person_id)})
        statement = 'INSERT INTO DriversLicense ?'
        driver.execute_lambda(lambda executor: executor.execute_statement(statement, convert_object_to_ion(new_license)))

        cursor = driver.execute_lambda(lambda executor: lookup_drivers_license_for_person(executor, person_id))
        try:
            next(cursor)
            logger.info('Successfully registered new driver.')
            return
        except StopIteration:
            logger.info('Problem occurred while inserting new license, please review the results.')
            return


def main(ledger_name=Constants.LEDGER_NAME):
    """
    Register a new driver's license.
    """
    try:
        with create_qldb_driver(ledger_name) as driver:
            person = {
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

            register_new_drivers_license(driver, person, drivers_license)
    except Exception as e:
        logger.exception('Error registering new driver.')
        raise e


if __name__ == '__main__':
    main()
