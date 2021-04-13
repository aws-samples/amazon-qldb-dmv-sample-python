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
import sys
from time import time
from unittest import TestCase

import pytest
from pyqldb.driver.qldb_driver import QldbDriver
from pyqldbsamples.add_secondary_owner import main as add_secondary_owner_main
from pyqldbsamples.connect_to_ledger import main as connect_to_ledger_main
from pyqldbsamples.constants import Constants
from pyqldbsamples.create_index import main as create_index_main
from pyqldbsamples.create_ledger import main as create_ledger_main
from pyqldbsamples.create_table import main as create_table_main
from pyqldbsamples.delete_ledger import main as delete_ledger_main
from pyqldbsamples.delete_ledger import delete_ledger, set_deletion_protection, wait_for_deleted
from pyqldbsamples.deletion_protection import main as deletion_protection_main
from pyqldbsamples.deletion_protection import LEDGER_NAME as deletion_protection_ledger_name
from pyqldbsamples.deregister_drivers_license import main as deregister_drivers_license_main
from pyqldbsamples.describe_journal_export import main as describe_journal_export_main
from pyqldbsamples.describe_ledger import main as describe_ledger_main
from pyqldbsamples.export_journal import main as export_journal_main
from pyqldbsamples.find_vehicles import main as find_vehicles_main
from pyqldbsamples.get_block import main as get_block_main
from pyqldbsamples.get_digest import main as get_digest_main
from pyqldbsamples.get_revision import main as get_revision_main
from pyqldbsamples.insert_document import main as insert_document_main
from pyqldbsamples.insert_ion_types import main as insert_ion_types_main
from pyqldbsamples.list_journal_exports import main as list_journal_exports_main
from pyqldbsamples.list_ledgers import main as list_ledgers_main
from pyqldbsamples.list_tables import main as list_tables_main
from pyqldbsamples.query_history import main as query_history_main
from pyqldbsamples.register_drivers_license import main as register_drivers_license_main
from pyqldbsamples.renew_drivers_license import main as renew_drivers_license_main
from pyqldbsamples.scan_table import main as scan_table_main
from pyqldbsamples.tag_resource import main as tag_resource_main
from pyqldbsamples.transfer_vehicle_ownership import main as transfer_vehicle_ownership_main
from pyqldbsamples.validate_qldb_hash_chain import main as validate_qldb_hash_chain_main
from pyqldbsamples.constants import Constants


@pytest.mark.usefixtures("config_variables")
class TestIntegration(TestCase):

    @classmethod
    def setUpClass(cls):
        force_delete_ledger(cls.ledger_name)
        force_delete_ledger(cls.ledger_name + "deletionProtection")
        force_delete_ledger(cls.ledger_name + "tags")

        create_ledger_main(cls.ledger_name)
        create_table_main(cls.ledger_name)
        poll_for_table_creation(cls.ledger_name)
        create_index_main(cls.ledger_name)
        insert_document_main(cls.ledger_name)

    @classmethod
    def tearDownClass(cls):
        delete_ledger_main(cls.ledger_name)

    def test_list_ledgers(self):
        list_ledgers_main()

    def test_connect_to_ledger(self):
        connect_to_ledger_main(self.ledger_name)

    def test_insert_ion_types(self):
        insert_ion_types_main(self.ledger_name)

    def test_scan_table(self):
        scan_table_main(self.ledger_name)

    def test_find_vehicles(self):
        find_vehicles_main(self.ledger_name)

    def test_add_secondary_owner(self):
        add_secondary_owner_main(self.ledger_name)

    def test_deregister_drivers_license(self):
        deregister_drivers_license_main(self.ledger_name)

    # def test_export_journal_and_describe_journal_export(self):
    #     export_id = export_journal_main().get('ExportId')
    #     sys.argv[1:] = [export_id]
    #     describe_journal_export_main()

    def test_describe_ledger(self):
        describe_ledger_main(self.ledger_name)

    def test_transfer_vehicle_ownership(self):
        transfer_vehicle_ownership_main(self.ledger_name)

    def test_query_history(self):
        query_history_main(self.ledger_name)

    def test_list_tables(self):
        list_tables_main(self.ledger_name)

    def test_register_drivers_license(self):
        register_drivers_license_main(self.ledger_name)

    def test_renew_drivers_license(self):
        renew_drivers_license_main(self.ledger_name)

    def test_deletion_protection(self):
        deletion_protection_main(self.ledger_name + "deletionProtection")

    def test_list_journal_exports(self):
        list_journal_exports_main(self.ledger_name)

    # def test_validate_qldb_hash_chain(self):
    #     validate_qldb_hash_chain_main(self.ledger_name)

    def test_get_revision(self):
        get_revision_main(self.ledger_name)

    def test_get_block(self):
        get_block_main(self.ledger_name)

    def test_get_digest(self):
        get_digest_main(self.ledger_name)

    def test_tag_resource(self):
        tag_resource_main(self.ledger_name + "tags")


def force_delete_ledger(ledger_name):
    try:
        set_deletion_protection(ledger_name, False)
        delete_ledger(ledger_name)
        wait_for_deleted(ledger_name)
    except Exception:
        pass


def poll_for_table_creation(ledger_name):
    driver = QldbDriver(ledger_name)
    max_poll_time = time() + 15
    while True:
        tables = driver.list_tables()
        count = len(list(tables))
        if count == 4 or time() > max_poll_time:
            break
