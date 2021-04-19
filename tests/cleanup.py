# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
from time import time, sleep
from sys import argv

from boto3 import resource
from pyqldb.driver.qldb_driver import QldbDriver

from pyqldbsamples.delete_ledger import delete_ledger, set_deletion_protection, wait_for_deleted


def poll_for_table_creation(ledger_name):
    driver = QldbDriver(ledger_name)
    max_poll_time = time() + 15
    while True:
        tables = driver.list_tables()
        count = len(list(tables))
        if count == 4 or time() > max_poll_time:
            break
        sleep(3)


def force_delete_ledger(ledger_name):
    try:
        set_deletion_protection(ledger_name, False)
        delete_ledger(ledger_name)
        wait_for_deleted(ledger_name)
    except Exception:
        pass


def force_delete_s3_bucket(bucket_name):
    s3_resource = resource('s3')
    bucket = s3_resource.Bucket(bucket_name)

    try:
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
    except Exception:
        pass


def force_delete_role(role_name):
    iam = resource('iam')
    role = iam.Role(role_name)

    try:
        role.delete()
    except Exception:
        pass


def force_delete_role_policies(role_name):
    iam_client = resource('iam')
    role = iam_client.Role(role_name)

    try:
        list_of_polices = list(role.attached_policies.all())
        for policy in list_of_polices:
            policy.detach_role(RoleName=role_name)

            policy = iam_client.Policy(policy.arn)
            policy.delete()
    except Exception:
        pass


def get_deletion_ledger_name(ledger_suffix):
    return 'py-dmv-' + ledger_suffix + '-delete'


def get_ledger_name(ledger_suffix):
    return 'py-dmv-' + ledger_suffix


def get_role_name(ledger_suffix):
    return 'github-actions-' + ledger_suffix + '-role'


def get_role_policy_name(ledger_suffix):
    return 'github-actions-' + ledger_suffix + '-policy'


def get_s3_bucket_name(ledger_suffix):
    return 'github-actions-' + ledger_suffix + '-bucket'


def get_tag_ledger_name(ledger_suffix):
    return 'py-dmv-' + ledger_suffix + '-tags'


def delete_resources(ledger_suffix):
    role_name = get_role_name(ledger_suffix)
    role_policy_name = get_role_policy_name(ledger_suffix)
    s3_bucket_name = get_s3_bucket_name(ledger_suffix)
    ledger_name = get_ledger_name(ledger_suffix)
    deletion_ledger_name = get_deletion_ledger_name(ledger_suffix)
    tag_ledger_name = get_deletion_ledger_name(ledger_suffix)

    force_delete_role_policies(role_policy_name)
    force_delete_role(role_name)
    force_delete_s3_bucket(s3_bucket_name)

    force_delete_ledger(ledger_name)
    force_delete_ledger(deletion_ledger_name)
    force_delete_ledger(tag_ledger_name)


if __name__ == '__main__':
    if len(argv) > 1:
        ledger_suffix = argv[1]
        delete_resources(ledger_suffix)
