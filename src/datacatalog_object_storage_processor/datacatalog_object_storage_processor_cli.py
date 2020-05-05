import argparse
import logging
import sys

from datacatalog_object_storage_processor.object_storage_processor import \
    ObjectStorageProcessor


class DatacatalogObjectStorageProcessorCLI:

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()
        cls._parse_args(argv)

    @classmethod
    def add_object_storage_parser(cls, subparsers):
        object_storage_parser = subparsers.add_parser('object-storage',
                                                      help='Object Storage commands')
        object_storage_subparsers = object_storage_parser.add_subparsers()
        sync_entries_parser = object_storage_subparsers.add_parser('sync-entries',
                                                                   help='Synchronize Entries')
        cls.__add_common_args(sync_entries_parser)
        sync_entries_parser.set_defaults(func=cls.__sync_entries)
        delete_entries_parser = object_storage_subparsers.add_parser('delete-entries',
                                                                     help='Delete Entries')
        cls.__add_common_args(delete_entries_parser)
        delete_entries_parser.set_defaults(func=cls.__delete_entries)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)

        parser.set_defaults(func=lambda *inner_args: logging.info('Must use a subcommand'))

        subparsers = parser.add_subparsers()

        cls.add_object_storage_parser(subparsers)

        args = parser.parse_args(argv)
        args.func(args)

    @classmethod
    def __add_common_args(cls, entries_parser):
        entries_parser.add_argument('--type',
                                    help='Object Storage type, '
                                    'supported values (cloud-storage,)',
                                    required=True)
        entries_parser.add_argument('--project-id', help='Project id', required=True)
        entries_parser.add_argument('--entry-group-name',
                                    help='Name of the Entry Group,'
                                    'used as a container for the object storage enties'
                                    'i.e: '
                                    'projects/my-project/locations/us-central1/entryGroups/'
                                    'my-entry-group',
                                    required=True)
        entries_parser.add_argument('--bucket-prefix',
                                    help='Specify a bucket prefix if you want to avoid scanning'
                                    ' too many GCS buckets')

    @classmethod
    def __sync_entries(cls, args):
        ObjectStorageProcessor(args.type,
                               args.project_id).sync_entries(args.entry_group_name,
                                                             args.bucket_prefix)

    @classmethod
    def __delete_entries(cls, args):
        ObjectStorageProcessor(args.type, args.project_id).delete_entries(args.entry_group_name)


def main():
    argv = sys.argv
    DatacatalogObjectStorageProcessorCLI.run(argv[1:] if len(argv) > 0 else argv)
