from unittest import TestCase
from unittest import mock

from datacatalog_object_storage_processor import datacatalog_object_storage_processor_cli


class TagManagerCLITest(TestCase):
    __PATCHED_OBJECT_STORAGE_PROCESSOR = 'datacatalog_object_storage_processor' \
                                        '.object_storage_processor.ObjectStorageProcessor'

    def test_parse_args_invalid_subcommand_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, datacatalog_object_storage_processor_cli.
            DatacatalogObjectStorageProcessorCLI._parse_args, ['invalid-subcommand'])

    def test_parse_args_sync_entries_missing_mandatory_args_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, datacatalog_object_storage_processor_cli.
            DatacatalogObjectStorageProcessorCLI._parse_args, ['object-storage', 'sync-entries'])

    def test_run_no_args_should_not_raise_system_exit(self):
        datacatalog_object_storage_processor_cli.DatacatalogObjectStorageProcessorCLI.run({})

    @mock.patch(f'{__PATCHED_OBJECT_STORAGE_PROCESSOR}.__init__', lambda self, *args: None)
    @mock.patch(f'{__PATCHED_OBJECT_STORAGE_PROCESSOR}.sync_entries')
    def test_run_sync_entries_with_args_should_not_raise_exception(self, sync_entries):
        datacatalog_object_storage_processor_cli.DatacatalogObjectStorageProcessorCLI.run([
            'object-storage', 'sync-entries', '--type', 'cloud_storage', '--project-id',
            'my-project', '--entry-group-name', 'my-entry-group'
        ])
        sync_entries.assert_called_once()

    @mock.patch(f'{__PATCHED_OBJECT_STORAGE_PROCESSOR}.__init__', lambda self, *args: None)
    @mock.patch(f'{__PATCHED_OBJECT_STORAGE_PROCESSOR}.delete_entries')
    def test_run_delete_entries_with_args_should_not_raise_exception(self, delete_entries):
        datacatalog_object_storage_processor_cli.DatacatalogObjectStorageProcessorCLI.run([
            'object-storage', 'delete-entries', '--type', 'cloud_storage', '--project-id',
            'my-project', '--entry-group-name', 'my-entry-group'
        ])
        delete_entries.assert_called_once()
