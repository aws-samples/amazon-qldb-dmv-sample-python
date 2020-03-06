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
from datetime import datetime
from decimal import Decimal
from logging import basicConfig, getLogger, INFO

from amazon.ion.simple_types import IonPyBool, IonPyBytes, IonPyDecimal, IonPyDict, IonPyFloat, IonPyInt, IonPyList, \
    IonPyNull, IonPySymbol, IonPyText, IonPyTimestamp
from amazon.ion.simpleion import loads
from amazon.ion.symbols import SymbolToken
from amazon.ion.core import IonType

from pyqldbsamples.create_table import create_table
from pyqldbsamples.insert_document import insert_documents
from pyqldbsamples.model.sample_data import convert_object_to_ion
from pyqldbsamples.connect_to_ledger import create_qldb_session

logger = getLogger(__name__)
basicConfig(level=INFO)

TABLE_NAME = 'IonTypes'


def update_record_and_verify_type(transaction_executor, parameter, ion_object, ion_type):
    """
    Update a record in the database table. Then query the value of the record and verify correct ion type saved.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type parameter: :py:class:`amazon.ion.simple_types.IonPyValue`
    :param parameter: The Ion value or Python native type that is convertible to Ion for filling in parameters of the
                      statement.

    :type ion_object: :py:obj:`IonPyBool`/:py:obj:`IonPyBytes`/:py:obj:`IonPyDecimal`/:py:obj:`IonPyDict`
                      /:py:obj:`IonPyFloat`/:py:obj:`IonPyInt`/:py:obj:`IonPyList`/:py:obj:`IonPyNull`
                      /:py:obj:`IonPySymbol`/:py:obj:`IonPyText`/:py:obj:`IonPyTimestamp`
    :param ion_object: The Ion object to verify against.

    :type ion_type: :py:class:`amazon.ion.core.IonType`
    :param ion_type: The Ion type to verify against.

    :raises TypeError: When queried value is not an instance of Ion type.
    """
    update_query = 'UPDATE {} SET Name = ?'.format(TABLE_NAME)
    transaction_executor.execute_statement(update_query, parameter)
    logger.info('Updated record.')

    search_query = 'SELECT VALUE Name FROM {}'.format(TABLE_NAME)
    cursor = transaction_executor.execute_statement(search_query)

    for c in cursor:
        if not isinstance(c, ion_object):
            raise TypeError('The queried value is not an instance of {}'.format(ion_object.__name__))

        if c.ion_type is not ion_type:
            raise TypeError('The queried value type does not match {}'.format(ion_type))

    logger.info("Successfully verified value is instance of '{}' with type '{}'.".format(ion_object.__name__, ion_type))
    return cursor


def delete_table(transaction_executor, table_name):
    """
    Delete a table.

    :type transaction_executor: :py:class:`pyqldb.execution.executor.Executor`
    :param transaction_executor: An Executor object allowing for execution of statements within a transaction.

    :type table_name: str
    :param table_name: Name of the table to delete.

    :rtype: int
    :return: The number of changes to the database.
    """
    logger.info("Deleting '{}' table...".format(table_name))
    cursor = transaction_executor.execute_statement('DROP TABLE {}'.format(table_name))
    logger.info("'{}' table successfully deleted.".format(table_name))
    return len(list(cursor))


def insert_and_verify_ion_types(session):
    """
    Insert all the supported Ion types and Python values that are convertible to Ion into a ledger and verify that they
    are stored and can be retrieved properly, retaining their original properties.

    :type session: :py:class:`pyqldb.session.base_qldb_session.BaseQldbSession`
    :param session: A QLDB session object.
    """
    python_bytes = str.encode('hello')
    python_bool = True
    python_float = float('0.2')
    python_decimal = Decimal('0.1')
    python_string = "string"
    python_int = 1
    python_null = None
    python_datetime = datetime(2016, 12, 20, 5, 23, 43)
    python_list = [1, 2]
    python_dict = {"brand": "Ford"}

    ion_clob = convert_object_to_ion(loads('{{"This is a CLOB of text."}}'))
    ion_blob = convert_object_to_ion(python_bytes)
    ion_bool = convert_object_to_ion(python_bool)
    ion_decimal = convert_object_to_ion(python_decimal)
    ion_float = convert_object_to_ion(python_float)
    ion_int = convert_object_to_ion(python_int)
    ion_list = convert_object_to_ion(python_list)
    ion_null = convert_object_to_ion(python_null)
    ion_sexp = convert_object_to_ion(loads('(cons 1 2)'))
    ion_string = convert_object_to_ion(python_string)
    ion_struct = convert_object_to_ion(python_dict)
    ion_symbol = convert_object_to_ion(SymbolToken(text='abc', sid=123))
    ion_timestamp = convert_object_to_ion(python_datetime)

    ion_null_clob = convert_object_to_ion(loads('null.clob'))
    ion_null_blob = convert_object_to_ion(loads('null.blob'))
    ion_null_bool = convert_object_to_ion(loads('null.bool'))
    ion_null_decimal = convert_object_to_ion(loads('null.decimal'))
    ion_null_float = convert_object_to_ion(loads('null.float'))
    ion_null_int = convert_object_to_ion(loads('null.int'))
    ion_null_list = convert_object_to_ion(loads('null.list'))
    ion_null_sexp = convert_object_to_ion(loads('null.sexp'))
    ion_null_string = convert_object_to_ion(loads('null.string'))
    ion_null_struct = convert_object_to_ion(loads('null.struct'))
    ion_null_symbol = convert_object_to_ion(loads('null.symbol'))
    ion_null_timestamp = convert_object_to_ion(loads('null.timestamp'))

    session.execute_lambda(lambda transaction_executor: create_table(transaction_executor, TABLE_NAME)
                           and insert_documents(transaction_executor, TABLE_NAME, [{'Name': 'val'}])
                           and update_record_and_verify_type(transaction_executor, python_bytes, IonPyBytes,
                                                             IonType.BLOB)
                           and update_record_and_verify_type(transaction_executor, python_bool, IonPyBool,
                                                             IonType.BOOL)
                           and update_record_and_verify_type(transaction_executor, python_float, IonPyFloat,
                                                             IonType.FLOAT)
                           and update_record_and_verify_type(transaction_executor, python_decimal, IonPyDecimal,
                                                             IonType.DECIMAL)
                           and update_record_and_verify_type(transaction_executor, python_string, IonPyText,
                                                             IonType.STRING)
                           and update_record_and_verify_type(transaction_executor, python_int, IonPyInt,
                                                             IonType.INT)
                           and update_record_and_verify_type(transaction_executor, python_null, IonPyNull,
                                                             IonType.NULL)
                           and update_record_and_verify_type(transaction_executor, python_datetime,
                                                             IonPyTimestamp, IonType.TIMESTAMP)
                           and update_record_and_verify_type(transaction_executor, python_list, IonPyList,
                                                             IonType.LIST)
                           and update_record_and_verify_type(transaction_executor, python_dict, IonPyDict,
                                                             IonType.STRUCT)
                           and update_record_and_verify_type(transaction_executor, ion_clob, IonPyBytes,
                                                             IonType.CLOB)
                           and update_record_and_verify_type(transaction_executor, ion_blob, IonPyBytes,
                                                             IonType.BLOB)
                           and update_record_and_verify_type(transaction_executor, ion_bool, IonPyBool,
                                                             IonType.BOOL)
                           and update_record_and_verify_type(transaction_executor, ion_decimal, IonPyDecimal,
                                                             IonType.DECIMAL)
                           and update_record_and_verify_type(transaction_executor, ion_float, IonPyFloat,
                                                             IonType.FLOAT)
                           and update_record_and_verify_type(transaction_executor, ion_int, IonPyInt,
                                                             IonType.INT)
                           and update_record_and_verify_type(transaction_executor, ion_list, IonPyList,
                                                             IonType.LIST)
                           and update_record_and_verify_type(transaction_executor, ion_null, IonPyNull,
                                                             IonType.NULL)
                           and update_record_and_verify_type(transaction_executor, ion_sexp, IonPyList,
                                                             IonType.SEXP)
                           and update_record_and_verify_type(transaction_executor, ion_string, IonPyText,
                                                             IonType.STRING)
                           and update_record_and_verify_type(transaction_executor, ion_struct, IonPyDict,
                                                             IonType.STRUCT)
                           and update_record_and_verify_type(transaction_executor, ion_symbol, IonPySymbol,
                                                             IonType.SYMBOL)
                           and update_record_and_verify_type(transaction_executor, ion_timestamp,
                                                             IonPyTimestamp, IonType.TIMESTAMP)
                           and update_record_and_verify_type(transaction_executor, ion_null_clob, IonPyNull,
                                                             IonType.CLOB)
                           and update_record_and_verify_type(transaction_executor, ion_null_blob, IonPyNull,
                                                             IonType.BLOB)
                           and update_record_and_verify_type(transaction_executor, ion_null_bool, IonPyNull,
                                                             IonType.BOOL)
                           and update_record_and_verify_type(transaction_executor, ion_null_decimal,
                                                             IonPyNull, IonType.DECIMAL)
                           and update_record_and_verify_type(transaction_executor, ion_null_float, IonPyNull,
                                                             IonType.FLOAT)
                           and update_record_and_verify_type(transaction_executor, ion_null_int, IonPyNull,
                                                             IonType.INT)
                           and update_record_and_verify_type(transaction_executor, ion_null_list, IonPyNull,
                                                             IonType.LIST)
                           and update_record_and_verify_type(transaction_executor, ion_null_sexp, IonPyNull,
                                                             IonType.SEXP)
                           and update_record_and_verify_type(transaction_executor, ion_null_string, IonPyNull,
                                                             IonType.STRING)
                           and update_record_and_verify_type(transaction_executor, ion_null_struct, IonPyNull,
                                                             IonType.STRUCT)
                           and update_record_and_verify_type(transaction_executor, ion_null_symbol, IonPyNull,
                                                             IonType.SYMBOL)
                           and update_record_and_verify_type(transaction_executor, ion_null_timestamp,
                                                             IonPyNull, IonType.TIMESTAMP)
                           and delete_table(transaction_executor, TABLE_NAME),
                           lambda retry_attempt: logger.info('Retrying due to OCC conflict...'))


if __name__ == '__main__':
    """
    Insert all the supported Ion types and Python values that are convertible to Ion into a ledger and verify that they
    are stored and can be retrieved properly, retaining their original properties.
    """
    try:
        with create_qldb_session() as session:
            insert_and_verify_ion_types(session)
    except Exception:
        logger.exception('Error updating and validating Ion types.')
