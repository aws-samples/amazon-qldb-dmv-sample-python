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
from pyqldbsamples.model.sample_data import SampleData, convert_object_to_ion

logger = getLogger(__name__)
basicConfig(level=INFO)


def deregister_drivers_license(driver, license_number):
    """
    De-register a driver's license with the given license number.

    :type driver: :py:class:`pyqldb.driver.qldb_driver.QldbDriver`
    :param driver: An instance of the QldbDriver class.

    :type license_number: str
    :param license_number: The license number of the driver's license to de-register.
    """
    logger.info('De-registering license with license number: {}.'.format(license_number))
    statement = 'DELETE FROM DriversLicense AS d WHERE d.LicenseNumber = ?'
    parameter = convert_object_to_ion(license_number)
    cursor = driver.execute_lambda(lambda executor: executor.execute_statement(statement, parameter))

    try:
        # Check whether cursor is empty.
        next(cursor)
        logger.info("Successfully de-registered driver's license: {}.".format(license_number))
    except StopIteration:
        logger.error('Error de-registering license, license {} not found.'.format(license_number))


def main(ledger_name=Constants.LEDGER_NAME):
    """
    De-register a driver's license.
    """
    license_number = SampleData.DRIVERS_LICENSE[1]['LicenseNumber']

    try:
        with create_qldb_driver(ledger_name) as driver:
            deregister_drivers_license(driver, license_number)
    except Exception as e:
        logger.exception('Error deleting driver license.')
        raise e


if __name__ == '__main__':
    main()
