import logging

import pandas as pd

from datacatalog_object_storage_processor.object_storage.cloud_storage.storage_client_helper \
    import StorageClientHelper


class StorageProcessor:

    __STORAGE_SYSTEM = 'cloud_storage'

    def __init__(self, project_id):
        self.__storage_helper = StorageClientHelper(project_id)
        self.__project_id = project_id

    def create_object_storage_data(self, bucket_prefix=None):
        logging.info('===> Get all Buckets from Cloud Storage...')
        buckets = self.__storage_helper.list_buckets(bucket_prefix)
        logging.info('==== DONE ==================================================')
        logging.info('')

        dataframe = None
        bucket_stats = []
        for bucket in buckets:
            bucket_name = bucket.name
            logging.info(f'[BUCKET: {bucket_name}')
            logging.info('Get Files information from Cloud Storage...')
            blobs = self.__storage_helper.list_blobs(bucket)
            bucket_stats.append({'bucket_name': bucket_name, 'files': len(blobs)})
            if len(blobs) > 0:
                aux_dataframe = self.create_dataframe_from_blobs(bucket_name, blobs)
                if dataframe is not None:
                    dataframe = dataframe.append(aux_dataframe)
                else:
                    dataframe = aux_dataframe
            else:
                logging.info(f'No files found on bucket: {bucket_name}')

        return dataframe, bucket_stats

    @classmethod
    def create_dataframe_from_blobs(cls, bucket_name, blobs):
        dataframe = pd.DataFrame([[
            cls.__build_linked_resource(bucket_name,
                                        blob.name), bucket_name, cls.__STORAGE_SYSTEM, blob.name,
            cls.__extract_file_type(blob.name), blob.public_url, blob.size, blob.time_created,
            blob.updated
        ] for blob in blobs],
                                 columns=[
                                     'linked_resource', 'bucket_name', 'system', 'file_name',
                                     'file_type', 'public_url', 'size', 'time_created',
                                     'time_updated'
                                 ])
        return dataframe

    @classmethod
    def __build_linked_resource(cls, bucket_name, blob_name):
        return f'gs://{bucket_name}/{blob_name}'

    @classmethod
    def __extract_file_type(cls, file_name):
        file_type_at = file_name.rfind('.')
        if file_type_at != -1:
            return file_name[file_type_at + 1:]
        else:
            return 'unknown_file_type'
