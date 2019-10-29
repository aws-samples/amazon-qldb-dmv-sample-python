.. _guide_quickstart:

Quickstart
==========
Getting started with AmazonQLDB sample application is easy, but requires a few steps.

Installation
------------
Install the Python AmazonQLDB driver and other dependencies via :command:`pip`::

    pip install -r requirements.txt

Configuration
-------------
Before you can begin running the sample application, you should set up authentication
credentials. Credentials for your AWS account can be found in the
`IAM Console <https://console.aws.amazon.com/iam/home>`_. You can
create or use an existing user. Go to manage access keys and
generate a new set of keys.

If you have the `AWS CLI <http://aws.amazon.com/cli/>`_
installed, then you can use it to configure your credentials file::

    aws configure

Alternatively, you can create the credential file yourself. By default,
its location is at ``~/.aws/credentials``::

    [default]
    aws_access_key_id = YOUR_ACCESS_KEY
    aws_secret_access_key = YOUR_SECRET_KEY

You may also want to set a default region. This can be done in the
configuration file. By default, its location is at ``~/.aws/config``::

    [default]
    region=us-east-1

Alternatively, you can pass a ``region_name`` when creating the driver.

This sets up credentials for the default profile as well as a default
region to use when creating connections.

Using Sample Application
------------------------

The sample code creates a ledger with tables and indexes, and inserts some documents into those tables,
among other things. Each of the files in the sample app can be run in the following way::

    python create_ledger.py

The above example will build the CreateLedger class with the necessary dependencies and create a ledger named:
``vehicle-registration``. You may run other examples after creating a ledger.

