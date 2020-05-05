import logging

from datacatalog_object_storage_processor.datacatalog_helper import DataCatalogHelper
from datacatalog_object_storage_processor.object_storage.cloud_storage.storage_processor import \
    StorageProcessor


class ObjectStorageProcessor:
    # Default location.
    __LOCATION = 'us-central1'
    __FILE_PATTERN_REGEX = r'^gs:[\/][\/]([a-zA-Z-_\d*]+)[\/](.*)$'
    __ALLOWED_OBJECT_STORAGE_TYPES = ['cloud_storage']

    def __init__(self, object_storage_type, project_id):
        if object_storage_type not in self.__ALLOWED_OBJECT_STORAGE_TYPES:
            raise Exception('Invalid object storage type: {}'.format(object_storage_type))

        self.__storage_processor = StorageProcessor(project_id)
        self.__dacatalog_helper = DataCatalogHelper(project_id)
        self.__object_storage_type = object_storage_type
        self.__project_id = project_id

    def sync_entries(self, entry_group_name, bucket_prefix=None):
        logging.info(
            f'===> Starting Object Storage processor, type [{self.__object_storage_type}]')

        dataframe, _ = self.__storage_processor.create_object_storage_data(bucket_prefix)

        logging.info(f'===> {len(dataframe) if dataframe is not None else 0} Entries found...')
        logging.info('')

        if dataframe is not None:
            logging.info('===> Synchronize Entries on DataCatalog from Object Storage files...')
            self.__dacatalog_helper.sync_entries_from_dataframe(dataframe, entry_group_name,
                                                                self.__object_storage_type)
        else:
            logging.info('===> Nothing to Synchronize...')

        logging.info('==== DONE ==================================================')
        logging.info('')

    def delete_entries(self, entry_group_name):
        logging.info('===> Delete Entries on DataCatalog from Object Storage files...')
        self.__dacatalog_helper.delete_entries(entry_group_name, self.__object_storage_type)

        logging.info('==== DONE ==================================================')
        logging.info('')
