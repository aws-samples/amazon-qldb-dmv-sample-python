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


class JournalBlock:
    """
    Represents a JournalBlock that was recorded after executing a transaction in the ledger.
    """
    def __init__(self, block_address, transaction_id, block_timestamp, block_hash, entries_hash, previous_block_hash,
                 entries_hash_list, transaction_info, redaction_info, revisions):
        self.block_address = block_address
        self.transaction_id = transaction_id
        self.block_timestamp = block_timestamp
        self.block_hash = block_hash
        self.entries_hash = entries_hash
        self.previous_block_hash = previous_block_hash
        self.entries_hash_list = entries_hash_list
        self.transaction_info = transaction_info
        self.redaction_info = redaction_info
        self.revisions = revisions


def from_ion(ion_value):
    """
    Construct a new JournalBlock object from an IonStruct.

    :type ion_value: :py:class:`amazon.ion.simple_types.IonSymbol`
    :param ion_value: The IonStruct returned by QLDB that represents a journal block.

    :rtype: :py:class:`pyqldbsamples.qldb.journal_block.JournalBlock`
    :return: The constructed JournalBlock object.
    """
    block_address = ion_value.get('blockAddress')
    transaction_id = ion_value.get('transactionId')
    block_timestamp = ion_value.get('blockTimestamp')
    block_hash = ion_value.get('blockHash')
    entries_hash = ion_value.get('entriesHash')
    previous_block_hash = ion_value.get('previousBlockHash')
    entries_hash_list = ion_value.get('entriesHashList')
    transaction_info = ion_value.get('transactionInfo')
    redaction_info = ion_value.get('redactionInfo')
    revisions = ion_value.get('revisions')

    journal_block = JournalBlock(block_address, transaction_id, block_timestamp, block_hash, entries_hash,
                                 previous_block_hash, entries_hash_list, transaction_info, redaction_info, revisions)
    return journal_block
