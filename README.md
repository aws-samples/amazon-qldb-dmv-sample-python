# AmazonQLDB Samples

The samples in this project demonstrate several uses of Amazon Quantum Ledger Database (QLDB).

## Requirements

### Basic Configuration

You need to set up your AWS security credentials and config before the sample code is able
to connect to AWS. 

Set up credentials (in e.g. `~/.aws/credentials`):

```
[default]
aws_access_key_id = <your access key id>
aws_secret_access_key = <your secret key>
```

Set up a default region (in e.g. `~/.aws/config`):

```
[default]
region = us-east-1 <or other region>
```

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html#SettingUp.Q.GetCredentials) page for more information.


### Python 3.x

The examples require Python 3.x. Please see the link below for more detail to install Python 3.x:

* [Python 3.x Installation](https://www.python.org/downloads/)

## Installing the driver and dependencies

Install Python QLDB driver and other dependencies using pip:

```
pip install -r requirements.txt
```

## Running the Sample code

The sample code creates a ledger with tables and indexes, and inserts some documents into those tables,
among other things. Each of the examples in this project can be run in the following way:

```python
python create_ledger.py
```

The above example will build the CreateLedger class with the necessary dependencies and create a ledger named:
`vehicle-registration`. You may run other examples after creating a ledger.

## Documentation

Sphinx is used for documentation. You can generate HTML locally with the following:

```
$ pip install -r requirements-docs.txt
$ pip install -e .
$ cd docs
$ make html
```

### Release 1.0.0-rc.1 (October 28, 2019)

* Initial preview release of the QLDB Python Sample Application.

## License

This library is licensed under the MIT-0 License.
