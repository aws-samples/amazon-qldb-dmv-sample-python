# Amazon QLDB Python DMV Sample App

The samples in this project demonstrate several uses of Amazon Quantum Ledger Database (QLDB).

For our tutorial, see [Python and Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.python.html).

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

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

### Release 1.0.0

* Upgraded pyqldb version from v1.0.0-rc.2 to 2.0.0
* Used args for execute_statement instead of a list
* Added examples for native python data types

### Release 1.0.0-rc.2 (October 29, 2019)

* Fixes for small documentation issues.

### Release 1.0.0-rc.1 (October 28, 2019)

* Initial preview release of the QLDB Python Sample Application.

## License

This library is licensed under the MIT-0 License.
