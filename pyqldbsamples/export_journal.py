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
from datetime import datetime, timedelta
from logging import basicConfig, getLogger, INFO
from sys import argv
from time import sleep

from boto3 import client, resource
from botocore.exceptions import ClientError

from pyqldbsamples.constants import Constants
from pyqldbsamples.describe_journal_export import describe_journal_export


logger = getLogger(__name__)
basicConfig(level=INFO)
qldb_client = client('qldb')

MAX_RETRY_COUNT = 40
EXPORT_COMPLETION_POLL_PERIOD_SEC = 10
JOURNAL_EXPORT_TIME_WINDOW_MINUTES = 10
EXPORT_ROLE_NAME = "QLDBTutorialJournalExportRole"
ROLE_POLICY_NAME = "QLDBTutorialJournalExportRolePolicy"
ERROR_CODE = '404'
POLICY_TEMPLATE = """{
                     "Version": "2012-10-17", 
                     "Statement": [{statements}]
                  }"""
ASSUME_ROLE_POLICY = POLICY_TEMPLATE.replace("{statements}",
                                                      """{
                                                         "Effect": "Allow",
                                                         "Principal": {
                                                             "Service": ["qldb.amazonaws.com"]
                                                         },
                                                         "Action": ["sts:AssumeRole"]
                                                      }""")
EXPORT_ROLE_S3_STATEMENT_TEMPLATE = """{
                                        "Sid": "QLDBJournalExportPermission",
                                        "Effect": "Allow",
                                        "Action": ["s3:PutObjectAcl", "s3:PutObject"],
                                        "Resource": "arn:aws:s3:::{bucket_name}/*"
                                    }"""
EXPORT_ROLE_KMS_STATEMENT_TEMPLATE = """{
                                        "Sid": "QLDBJournalExportPermission",
                                        "Effect": "Allow",
                                        "Action": ["kms:GenerateDataKey"],
                                        "Resource": "{kms_arn}"
                                    }"""


def set_up_s3_encryption_configuration(kms_arn=None):
    """
    Use the default SSE-S3 configuration for the journal export if a KMS key ARN was not given.

    :type kms_arn: str
    :param kms_arn: The Amazon Resource Name to encrypt.

    :rtype: dict
    :return: The encryption configuration for JournalS3Export.
    """
    if kms_arn is None:
        return {'ObjectEncryptionType': 'SSE_S3'}

    return {'ObjectEncryptionType': {'S3ObjectEncryptionType': 'SSE_KMS', 'KmsKeyArn': kms_arn}}


def does_bucket_exists(bucket_name, s3_resource):
    """
    Check whether a bucket exists in S3.

    :type bucket_name: str
    :param bucket_name: The name of the bucket to check.

    :type s3_resource: :py:class:`botocore.client.BaseClient`
    :param s3_resource: A low-level S3 resource service client.

    :rtype: bool
    :return: If the bucket exists.
    """
    try:
        s3_resource.meta.client.head_bucket(Bucket=bucket_name)
    except ClientError as ce:
        error_code = ce.response['Error']['Code']
        if error_code == ERROR_CODE:
            return False
    return True


def create_s3_bucket_if_not_exists(bucket_name, s3_resource):
    """
    Create a S3 bucket if one with the given bucket_name does not exists.

    :type bucket_name: str
    :param bucket_name: The name of the bucket to check.

    :type s3_resource: :py:class:`botocore.client.BaseClient`
    :param s3_resource: A low-level S3 resource service client.
    """
    if not does_bucket_exists(bucket_name, s3_resource):
        logger.info('S3 bucket {} does not exist. Creating it now.'.format(bucket_name))
        s3_client = client('s3')
        s3_region = s3_client.meta.region_name
        try:
            s3_resource.create_bucket(Bucket=bucket_name,
                                      CreateBucketConfiguration={'LocationConstraint': s3_region})
            logger.info('Bucket with name: {} created.'.format(bucket_name))
        except Exception as e:
            logger.info('Unable to create S3 bucket named: {}'.format(bucket_name))
            raise e


def create_export_role(role_name, key_arn, role_policy_name, s3_bucket):
    """
    Create a new export rule and a new managed policy for the current AWS account.

    :type role_name: str
    :param role_name: The name of the role to be created.

    :type key_arn: str
    :param key_arn: The optional KMS Key ARN used to configure the role policy statement.

    :type role_policy_name: str
    :param role_policy_name: Name of the role policy to be created.

    :type s3_bucket: str
    :param s3_bucket: If key_arn is None, create a new ARN using the given bucket name.

    :rtype: str
    :return: The ARN of the newly created export role.
    """
    iam_client = client('iam')
    logger.info('Trying to retrieve role with name: {}.'.format(role_name))
    try:
        new_role_arn = iam_client.get_role(RoleName=role_name).get('Role').get('Arn')
        logger.info('The role called {} already exists.'.format(role_name))
    except iam_client.exceptions.NoSuchEntityException:
        logger.info('The role called {} does not exist. Creating it now.'.format(role_name))
        role = iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=ASSUME_ROLE_POLICY)
        new_role_arn = role.get('Role').get('Arn')

        role_policy_statement = EXPORT_ROLE_S3_STATEMENT_TEMPLATE.replace('{bucket_name}', s3_bucket)
        if key_arn is not None:
            role_policy_statement = "{}, {}".format(role_policy_statement,
                                                    EXPORT_ROLE_KMS_STATEMENT_TEMPLATE.replace('{kms_arn}', key_arn))
        role_policy = POLICY_TEMPLATE.replace('{statements}', role_policy_statement)

        create_policy_result = iam_client.create_policy(PolicyName=role_policy_name, PolicyDocument=role_policy)
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn=create_policy_result.get('Policy').get('Arn'))

        logger.info('Role {} created with ARN: {} and policy: {}.'.format(role_name, new_role_arn, role_policy))
    return new_role_arn


def wait_for_export_to_complete(name, export_id):
    """
    Wait for the journal export to complete.

    :type name: str
    :param name: Name of the ledger to wait on.

    :type export_id: str
    :param export_id: The unique export ID of the journal export.

    :rtype: dict
    :return: A description of the journal export.

    :raises RunTimeError: When the export fails to complete within a constant number of retries.
    """
    logger.info('Waiting for JournalS3Export for {} to complete...'.format(export_id))
    count = 0
    while count < MAX_RETRY_COUNT:
        export_description = describe_journal_export(name, export_id).get('ExportDescription')
        if export_description.get('Status') == "COMPLETED":
            logger.info('JournalS3Export completed.')
            return export_description
        logger.info('JournalS3Export is still in progress. Please wait.')
        sleep(EXPORT_COMPLETION_POLL_PERIOD_SEC)
        count += 1
    raise RuntimeError('Journal Export did not complete for {}.'.format(export_id))


def create_export_and_wait_for_completion(name, bucket, prefix, encryption_config, role_arn=None):
    """
    Request QLDB to export the contents of the journal for the given time period and S3 configuration. Before calling
    this function the S3 bucket should be created, see
    :py:class:`pyqldbsamples.export_journal.create_s3_bucket_if_not_exists`

    :type name: str
    :param name: Name of the ledger to create a journal export for.

    :type bucket: str
    :param bucket: S3 bucket to write the data to.

    :type prefix: str
    :param prefix: S3 prefix to be prefixed to the files being written.

    :type encryption_config: dict
    :param encryption_config: Encryption configuration for S3.

    :type role_arn: str
    :param role_arn: The IAM role ARN to be used when exporting the journal.

    :rtype: dict
    :return: The result of the request.
    """
    if role_arn is None:
        role_arn = create_export_role(EXPORT_ROLE_NAME, encryption_config.get('KmsKeyArn'), ROLE_POLICY_NAME, bucket)
    try:
        start_time = datetime.utcnow() - timedelta(minutes=JOURNAL_EXPORT_TIME_WINDOW_MINUTES)
        end_time = datetime.utcnow()

        result = create_export(name, start_time, end_time, bucket, prefix, encryption_config, role_arn)
        wait_for_export_to_complete(Constants.LEDGER_NAME, result.get('ExportId'))
        logger.info('JournalS3Export for exportId {} is completed.'.format(result.get('ExportId')))
        return result
    except Exception as e:
        logger.exception('Unable to create an export!')
        raise e


def create_export(ledger_name, start_time, end_time, s3_bucket_name, s3_prefix, encryption_configuration, role_arn):
    """
    Request QLDB to export the contents of the journal for the given time period and S3 configuration. Before calling
    this function the S3 bucket should be created,
    see :py:meth:`pyqldbsamples.export_journal.create_s3_bucket_if_not_exists`

    :type ledger_name: str
    :param ledger_name: Name of the ledger.

    :type start_time: :py:class:`datetime.datetime`
    :param start_time: Time from when the journal contents should be exported.

    :type end_time: :py:class:`datetime.datetime`
    :param end_time: Time until which the journal contents should be exported.

    :type s3_bucket_name: str
    :param s3_bucket_name: S3 bucket to write the data to.

    :type s3_prefix: str
    :param s3_prefix: S3 prefix to be prefixed to the files written.

    :type encryption_configuration: dict
    :param encryption_configuration: Encryption configuration for S3.

    :type role_arn: str
    :param role_arn: The IAM role ARN to be used when exporting the journal.

    :rtype: dict
    :return: The result of the request.
    """
    logger.info("Let's create a journal export for ledger with name: {}.".format(ledger_name))
    try:
        result = qldb_client.export_journal_to_s3(Name=ledger_name, InclusiveStartTime=start_time,
                                                  ExclusiveEndTime=end_time,
                                                  S3ExportConfiguration={'Bucket': s3_bucket_name, 'Prefix': s3_prefix,
                                                                         'EncryptionConfiguration':
                                                                             encryption_configuration},
                                                  RoleArn=role_arn)
        logger.info("Requested QLDB to export contents of the journal.")
        return result
    except qldb_client.exceptions.InvalidParameterException as ipe:
        logger.error("The eventually consistent behavior of the IAM service may cause this export to fail its first"
                     " attempts, please retry.")
        raise ipe


def main(ledger_name=Constants.LEDGER_NAME):
    """
    Export a journal to S3.

    This code requires an S3 bucket. You can provide the name of an S3 bucket that
    you wish to use via the arguments (args[0]). The code will check if the bucket
    exists and create it if not. If you don't provide a bucket name, the code will
    create a unique bucket for the purposes of this tutorial.

    Optionally, you can provide an IAM role ARN to use for the journal export via
    the arguments (args[1]). Otherwise, the code will create and use a role named
    "QLDBTutorialJournalExportRole".

    S3 Export Encryption:
    Optionally, you can provide a KMS key ARN to use for S3-KMS encryption, via
    the arguments (args[2]). The tutorial code will fail if you provide a KMS key
    ARN that doesn't exist.

    If KMS Key ARN is not provided, the Tutorial Code will use
    SSE-S3 for the S3 Export.

    If provided, the target KMS Key is expected to have at least the following
    KeyPolicy:
    -------------
    CustomCmkForQLDBExportEncryption:
       Type: AWS::KMS::Key
       Properties:
         KeyUsage: ENCRYPT_DECRYPT
         KeyPolicy:
           Version: "2012-10-17"
           Id: key-default-1
           Statement:
           - Sid: Grant Permissions for QLDB to use the key
             Effect: Allow
             Principal:
               Service: qldb.amazonaws.com
             Action:
               - kms:Encrypt
               - kms:GenerateDataKey
             # In a key policy, you use "*" for the resource, which means "this CMK."
             # A key policy applies only to the CMK it is attached to.
             Resource: '*'
    -------------
    Please see the KMS key policy developer guide here:
    https://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html
    """
    s3_resource = resource('s3')
    sts = client('sts')

    kms_arn = None
    role_arn = None

    if len(argv) >= 2:
        s3_bucket_name = argv[1]
        if len(argv) >= 3:
            role_arn = argv[2]
        if len(argv) == 4:
            kms_arn = argv[3]
    else:
        identity = sts.get_caller_identity()
        s3_bucket_name = "{}-{}".format(Constants.JOURNAL_EXPORT_S3_BUCKET_NAME_PREFIX, identity['Account'])

    create_s3_bucket_if_not_exists(s3_bucket_name, s3_resource)

    s3_encryption_config = set_up_s3_encryption_configuration(kms_arn)

    return create_export_and_wait_for_completion(ledger_name, s3_bucket_name, ledger_name + '/',
                                                 s3_encryption_config, role_arn)


if __name__ == '__main__':
    main()
