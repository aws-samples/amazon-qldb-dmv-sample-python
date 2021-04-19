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
from pyqldbsamples.create_ledger import wait_for_active

logger = getLogger(__name__)
basicConfig(level=INFO)
qldb_client = client('qldb')

ADD_TAGS = {'Domain': 'Prod'}
CREATE_TAGS = {'IsTest': 'true', 'Domain': 'Test'}
REMOVE_TAGS = ['IsTest']


def create_with_tags(name, tags):
    """
    Create a ledger with the specified name and the given tags.

    :type name: str
    :param name: Name of the ledger to be created.

    :type tags: dict
    :param tags: A dictionary of tags to create the ledger with.

    :rtype: dict
    :return: The result returned by the database.
    """
    logger.info("Let's create the ledger with name: {}...".format(name))
    result = qldb_client.create_ledger(Name=name, Tags=tags, PermissionsMode="ALLOW_ALL")
    logger.info('Success. Ledger state: {}.'.format(result.get('State')))
    return result


def tag_resource(resource_arn, tags):
    """
    Add one or more tags to the specified QLDB resource.

    :type resource_arn: dict
    :param resource_arn: The Amazon Resource Name (ARN) of the ledger to which to add tags.

    :type tags: dict
    :param tags: The key-value pairs to add as tags.
    """
    logger.info("Let's add tags {} for resource with arn: {}...".format(tags, resource_arn))
    qldb_client.tag_resource(ResourceArn=resource_arn, Tags=tags)
    logger.info('Successfully added tags.')


def untag_resource(resource_arn, tag_keys):
    """
    Remove one or more tags from the specified QLDB resource.

    :type resource_arn: dict
    :param resource_arn: The Amazon Resource Name (ARN) of the ledger from which to remove the tags.

    :type tag_keys: list
    :param tag_keys: The list of tag keys to remove.
    """
    logger.info("Let's remove tags {} for resource with arn: {}.".format(tag_keys, resource_arn))
    qldb_client.untag_resource(ResourceArn=resource_arn, TagKeys=tag_keys)
    logger.info('Successfully removed tags.')


def list_tags(resource_arn):
    """
    Returns all tags for a specified Amazon QLDB resource.

    :type resource_arn: dict
    :param resource_arn: The Amazon Resource Name (ARN) for which to list tags off.

    :rtype: dict
    :return: All tags on the specified resource.
    """
    logger.info("Let's list the tags for resource with arn: {}.".format(resource_arn))
    result = qldb_client.list_tags_for_resource(ResourceArn=resource_arn)
    logger.info('Success. Tags: {}.'.format(result.get('Tags')))
    return result


def main(ledger_name=Constants.LEDGER_NAME_WITH_TAGS):
    """
    Tagging and un-tagging resources, including tag on create.
    """
    try:
        result = create_with_tags(ledger_name, CREATE_TAGS)
        wait_for_active(ledger_name)
        ARN = result.get('Arn')
        list_tags(ARN)
        untag_resource(ARN, REMOVE_TAGS)
        list_tags(ARN)
        tag_resource(ARN, ADD_TAGS)
        list_tags(ARN)
    except Exception as e:
        logger.exception('Unable to tag or untag resources!')
        raise e


if __name__ == '__main__':
    main()
