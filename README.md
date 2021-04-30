# Amazon QLDB Python DMV Sample App
[![license](https://img.shields.io/badge/license-MIT-green)](https://github.com/aws-samples/amazon-qldb-dmv-sample-python/blob/master/LICENSE)
[![AWS Provider](https://img.shields.io/badge/provider-AWS-orange?logo=amazon-aws&color=ff9900)](https://aws.amazon.com/qldb/)

The samples in this project demonstrate several uses of Amazon Quantum Ledger Database (QLDB).

For our tutorial, see [Python and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.python.html).

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

### Required Python versions

DMV Sample App v1.x requires Python 3.4 or later.

DMV Sample App v2.x requires Python 3.6 or later. 

Please see the link below for more detail to install Python:

* [Python Installation](https://www.python.org/downloads/)

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

## License

This library is licensed under the MIT-0 License.
