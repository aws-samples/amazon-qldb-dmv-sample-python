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
from time import sleep

from pyqldbsamples.add_secondary_owner import get_document_ids, SampleData
from pyqldbsamples.connect_to_ledger import create_qldb_driver
from pyqldbsamples.constants import Constants
from pyqldbsamples.model.sample_data import print_ion
from pyqldbsamples.transfer_vehicle_ownership import validate_and_update_registration

logger = getLogger(__name__)
basicConfig(level=INFO)


def get_table_id(transaction_executor, table_name):
    """
    Get the tableId of a table with the given table name.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements
     within a transaction.

    :type table_name: :py:class:`amazon.ion.simple_types.IonPyText`
    :param table_name: The table name for which we'll get the tableId.

    :rtype: :py:class:`amazon.ion.simple_types.IonPyText`
    :return: The tableId for the provided table name.
    """
    logger.info("Getting the tableId for table with name: {}".format(table_name))
    query = "SELECT VALUE tableId FROM information_schema.user_tables WHERE name = '{}'".format(table_name)
    results = list(transaction_executor.execute_statement(query))
    logger.info("Results list: {}".format(results))
    if len(results) == 0:
        raise Exception("Unable to find table with name {}".format(table_name))
    return results[0]


def get_historic_registration_by_owner_id(transaction_executor, registration_document_id, owner_document_id):
    """
    Get the historic revision of a vehicle registration with the given ownerId.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements
     within a transaction.

    :type registration_document_id: :py:class:`amazon.ion.simple_types.IonPyText`
    :param registration_document_id: The QLDB DocumentId of the vehicle registration document.

    :type owner_document_id: :py:class:`amazon.ion.simple_types.IonPyText`
    :param owner_document_id: The QLDB DocumentId of the vehicle owner.

    :rtype :py:class:`amazon.ion.simple_types.IonPySymbol`
    :return An Ion Struct returned by QLDB that represents a document revision.
    """
    logger.info(
        "Querying the 'VehicleRegistration' table's history for a registration with documentId: {} and owner: {}"
        .format(registration_document_id, owner_document_id)
    )
    query = "SELECT * FROM history({}) AS h WHERE h.metadata.id = '{}' AND h.data.Owners.PrimaryOwner.PersonId = '{}'" \
        .format(Constants.VEHICLE_REGISTRATION_TABLE_NAME, registration_document_id, owner_document_id)
    results = list(transaction_executor.execute_statement(query))
    if len(results) == 0:
        raise Exception(
            "Unable to find historic registration with documentId: {} and ownerId: {}"
            .format(registration_document_id, owner_document_id)
        )
    elif len(results) > 1:
        raise Exception(
            "Found more than 1 historic registrations with documentId: {} and ownerId: {}"
            .format(registration_document_id, owner_document_id)
        )
    result = results[0]
    print_ion(result)
    return result


def get_historic_registration_by_version_number(transaction_executor, registration_document_id, version_number):
    """
    Get the historic revision of a vehicle registration with the given document version.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements
     within a transaction.

    :type registration_document_id: :py:class:`amazon.ion.simple_types.IonPyText`
    :param registration_document_id: The QLDB DocumentId of the vehicle registration document.

    :type version_number: :py:class:`amazon.ion.simple_types.IonPyInt`
    :param version_number: The version of the vehicle registration document to query the history for.

    :rtype :py:class:`amazon.ion.simple_types.IonPySymbol`
    :return An Ion Struct returned by QLDB that represents a document revision.
    """
    logger.info(
        "Querying the 'VehicleRegistration' table's history for a registration with documentId: {} and version: {}"
        .format(registration_document_id, version_number)
    )
    query = "SELECT * FROM history({}) AS h WHERE h.metadata.id = '{}' AND h.metadata.version = {}" \
        .format(Constants.VEHICLE_REGISTRATION_TABLE_NAME, registration_document_id, version_number)
    results = list(transaction_executor.execute_statement(query))
    if len(results) == 0:
        raise Exception(
            "Unable to find historic registration with documentId: {} and version: {}"
            .format(registration_document_id, version_number)
        )
    result = results[0]
    print_ion(result)
    return result


def redact_previous_registration(transaction_executor, vin, previous_owner_gov_id):
    """
    Redact a historic revision of a vehicle registration.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements
     within a transaction.

    :type vin: :py:class:`amazon.ion.simple_types.IonPyText`
    :param vin: The VIN specified in the vehicle registration.

    :type previous_owner_gov_id: :py:class:`amazon.ion.simple_types.IonPyText`
    :param previous_owner_gov_id: The OwnerId on the previous revision of the
     vehicle registration that is going to be redacted.

    :rtype :py:class:`amazon.ion.simple_types.IonPySymbol`
    :return An Ion Struct returned as a response to a redaction request.
    """
    table_id = get_table_id(transaction_executor, Constants.VEHICLE_REGISTRATION_TABLE_NAME)
    registration_document_id = \
        get_document_ids(transaction_executor, Constants.VEHICLE_REGISTRATION_TABLE_NAME, 'VIN', vin)[0]
    previous_owner_document_id = \
        get_document_ids(transaction_executor, Constants.PERSON_TABLE_NAME, 'GovId', previous_owner_gov_id)[0]
    historic_revision_block_address = get_historic_registration_by_owner_id(
        transaction_executor, registration_document_id, previous_owner_document_id
    )['blockAddress']

    logger.info("Redacting the revision at blockAddress: {} with tableId: {} and documentId: {}"
                .format(historic_revision_block_address, table_id, registration_document_id))
    redact_query = "EXEC redact_revision ?, '{}', '{}'".format(table_id, registration_document_id)
    redact_request = next(transaction_executor.execute_statement(redact_query, historic_revision_block_address))
    print_ion(redact_request)
    return redact_request


def wait_till_revision_redacted(driver, redact_request):
    """
    Wait until a revision is redacted by checking the history of the document.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type redact_request: :py:class:`amazon.ion.simple_types.IonPySymbol`
    :param redact_request: An Ion Struct returned as a response to a redaction request.
    """
    is_redacted = False
    while not is_redacted:
        revision = driver.execute_lambda(lambda executor: get_historic_registration_by_version_number(
            executor, redact_request['documentId'], redact_request['version']
        ))
        if 'data' not in revision and 'dataHash' in revision:
            is_redacted = True
            logger.info("Revision was successfully redacted.")
        else:
            logger.info("Revision is not yet redacted. Waiting for sometime...")
            sleep(10)


def main(ledger_name=Constants.LEDGER_NAME):
    """
    Redact a historic revision of a vehicle registration document.
    """
    vehicle_vin = SampleData.VEHICLE[2]['VIN']
    previous_owner = SampleData.PERSON[2]['GovId']
    new_owner = SampleData.PERSON[3]['GovId']

    try:
        with create_qldb_driver(ledger_name) as driver:
            validate_and_update_registration(driver, vehicle_vin, previous_owner, new_owner)
            redact_request = driver.execute_lambda(
                lambda executor: redact_previous_registration(executor, vehicle_vin, previous_owner))
            wait_till_revision_redacted(driver, redact_request)
    except Exception as e:
        logger.exception('Error redacting VehicleRegistration.')
        raise e


if __name__ == '__main__':
    main()
