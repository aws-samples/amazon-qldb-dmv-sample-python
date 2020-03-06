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
import os
import re
import setuptools

ROOT = os.path.join(os.path.dirname(__file__), 'pyqldbsamples')
VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.a-z\-]+)['"]''')
requires = ['amazon.ion>=0.5.0,<0.6',
            'boto3>=1.9.237,<2',
            'botocore>=1.12.237,<2',
            'pyqldb>=2.0.0,<3'
            ]


def get_version():
    init = open(os.path.join(ROOT, '__init__.py')).read()
    return VERSION_RE.search(init).group(1)


setuptools.setup(
    name='pyqldbsamples',
    version=get_version(),
    description='Sample app for Amazon QLDB',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Amazon Web Services',
    packages=setuptools.find_packages(),
    install_requires=requires,
    license="Apache License 2.0"
)
