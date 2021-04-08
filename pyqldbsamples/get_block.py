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
from pyqldbsamples.model.sample_data import SampleData
from pyqldbsamples.get_revision import lookup_registration_for_vin
from pyqldbsamples.get_digest import get_digest_result
from pyqldbsamples.verifier import to_base_64, verify_document, parse_block, flip_random_bit
from pyqldbsamples.qldb.block_address import block_address_to_dictionary
from pyqldbsamples.connect_to_ledger import create_qldb_driver
from pyqldbsamples.qldb.qldb_string_utils import block_response_to_string, value_holder_to_string

logger = getLogger(__name__)
basicConfig(level=INFO)
qldb_client = client('qldb')


def get_block(ledger_name, block_address):
    """
    Get the block of a ledger's journal.

    :type ledger_name: str
    :param ledger_name: Name of the ledger to operate on.

    :type block_address: dict
    :param block_address: The location of the block to request.

    :rtype: dict
    :return: The response of the request.
    """
    logger.info("Let's get the block for block address {} of the ledger named {}.".format(block_address, ledger_name))
    result = qldb_client.get_block(Name=ledger_name, BlockAddress=block_address)
    logger.info('Success. GetBlock: {}'.format(block_response_to_string(result)))
    return result


def get_block_with_proof(ledger_name, block_address, digest_tip_address):
    """
    Get the block of a ledger's journal. Also returns a proof of the block for verification.

    :type ledger_name: str
    :param ledger_name: Name of the ledger to operate on.

    :type block_address: dict
    :param block_address: The location of the block to request.

    :type digest_tip_address: dict
    :param digest_tip_address: The location of the digest tip.

    :rtype: dict
    :return: The response of the request.
    """
    logger.info("Let's get the block for block address {}, digest tip address {}, for the ledger named {}.".format(
        block_address, digest_tip_address, ledger_name))
    result = qldb_client.get_block(Name=ledger_name, BlockAddress=block_address,
                                   DigestTipAddress=digest_tip_address)
    logger.info('Success. GetBlock: {}.'.format(block_response_to_string(result)))
    return result


def verify_block(ledger_name, block_address):
    """
    Verify block by validating the proof returned in the getBlock response.

    :type ledger_name: str
    :param ledger_name: The ledger to get digest from.

    :type block_address: str/:py:class:`amazon.ion.simple_types.IonPyDict`
    :param block_address: The address of the block to verify.

    :raises AssertionError: When verification failed.
    """
    logger.info("Let's verify blocks for ledger with name={}.".format(ledger_name))

    try:
        logger.info("First, let's get a digest.")
        digest_result = get_digest_result(ledger_name)

        digest_tip_address = digest_result.get('DigestTipAddress')
        digest_bytes = digest_result.get('Digest')

        logger.info('Got a ledger digest. Digest end address={}, digest={}'.format(
                    value_holder_to_string(digest_tip_address.get('IonText')), to_base_64(digest_bytes)))
        get_block_result = get_block_with_proof(ledger_name, block_address_to_dictionary(block_address),
                                                digest_tip_address)
        block = get_block_result.get('Block')
        block_hash = parse_block(block)

        verified = verify_document(block_hash, digest_bytes, get_block_result.get('Proof'))

        if not verified:
            raise AssertionError('Block is not verified!')
        else:
            logger.info('Success! The block is verified.')

        altered_digest = flip_random_bit(digest_bytes)
        logger.info("Let's try flipping one bit in the digest and assert that the block is NOT verified. "
                    "The altered digest is: {}".format(to_base_64(altered_digest)))

        verified = verify_document(block_hash, altered_digest, get_block_result.get('Proof'))

        if verified:
            raise AssertionError('Expected block to not be verified against altered digest.')
        else:
            logger.info('Success! As expected flipping a bit in the digest causes verification to fail.')

        altered_block_hash = flip_random_bit(block_hash)
        logger.info("Let's try flipping one bit in the block's hash and assert that the block is NOT verified. "
                    "The altered block hash is: {}.".format(to_base_64(altered_block_hash)))

        verified = verify_document(altered_block_hash, digest_bytes, get_block_result.get('Proof'))

        if verified:
            raise AssertionError('Expected altered block hash to not be verified against digest.')
        else:
            logger.info('Success! As expected flipping a bit in the block hash causes verification to fail.')
    except Exception as e:
        logger.exception('Failed to verify blocks in the ledger with name={}.'.format(ledger_name))
        raise e


if __name__ == '__main__':
    """
    Get a journal block from a QLDB ledger.

    After getting the block, we get the digest of the ledger and validate the
    proof returned in the getBlock response.
    """
    vin = SampleData.VEHICLE_REGISTRATION[1]['VIN']
    try:
        with create_qldb_driver() as driver:
            cursor = lookup_registration_for_vin(driver, vin)
            row = next(cursor)
            block_address = row.get('blockAddress')
            verify_block(Constants.LEDGER_NAME, block_address)
    except Exception:
        logger.exception('Unable to query vehicle registration by Vin.')
