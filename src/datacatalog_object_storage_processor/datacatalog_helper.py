import logging
import re
import timeit

import pandas as pd
from google.api_core import exceptions
from google.cloud import datacatalog_v1

from datacatalog_object_storage_processor import datacatalog_entity_factory
from datacatalog_object_storage_processor import utils


class DataCatalogHelper:
    """
    DataCatalogHelper enables calls to datacatalog_v1
    """

    __LOCATION = 'us-central1'
    __TAG_TEMPLATE = 'object_storage_entries_sync_details'

    def __init__(self, project_id):
        self.__datacatalog = datacatalog_v1.DataCatalogClient()
        self.__project_id = project_id

    def create_tag_template(self, tag_template_name):
        tag_template = datacatalog_v1.types.TagTemplate()
        tag_template.display_name = 'Tag Template with details of ingested object storage ' \
                                    'entries - all entries are a snapshot of the execution time'

        tag_template.fields['execution_time'].display_name = \
            'Sync Execution time'
        tag_template.fields['execution_time'].type.primitive_type = \
            datacatalog_v1.enums.FieldType.PrimitiveType.TIMESTAMP.value

        tag_template.fields['bucket_name'].display_name = \
            'Bucket Name'
        tag_template.fields['bucket_name'].type.primitive_type = \
            datacatalog_v1.enums.FieldType.PrimitiveType.STRING.value

        tag_template.fields['file_url'].display_name = \
            'File URL'
        tag_template.fields['file_url'].type.primitive_type = \
            datacatalog_v1.enums.FieldType.PrimitiveType.STRING.value

        tag_template.fields['file_name'].display_name = \
            'File Name'
        tag_template.fields['file_name'].type.primitive_type = \
            datacatalog_v1.enums.FieldType.PrimitiveType.STRING.value

        tag_template.fields['file_size'].display_name = \
            'File Size'
        tag_template.fields['file_size'].type.primitive_type = \
            datacatalog_v1.enums.FieldType.PrimitiveType.DOUBLE.value

        project_id, location_id, tag_template_id = \
            self.extract_resources_from_template(tag_template_name)

        return self.__datacatalog.create_tag_template(
            parent=datacatalog_v1.DataCatalogClient.location_path(project_id, location_id),
            tag_template_id=tag_template_id,
            tag_template=tag_template)

    def sync_entries_from_dataframe(self, dataframe, entry_group_name, system):
        execution_time = pd.Timestamp.utcnow()

        project_id, location_id, entry_group_id = \
            self.extract_resources_from_entry_group(entry_group_name)

        logging.info(f'[PROJECT: {project_id}]')
        logging.info(f'[LOCATION: {location_id}]')
        logging.info(f'[ENTRY_GROUP: {entry_group_id}]')
        logging.info('')

        self.__load_entry_group(entry_group_id, entry_group_name, location_id, project_id)

        resolved_tag_template_name = self.__load_tag_template()

        logging.info(f'===> Creating Entries on project: {self.__project_id}...')
        logging.info('')

        self.__sync_entries_from_dataframe(dataframe, entry_group_name, resolved_tag_template_name,
                                           execution_time, system)

    def delete_entries(self, entry_group_name, system):
        self.delete_obsolete_metadata([], system, entry_group_name)

    def create_entry_group(self, project_id, location_id, entry_group_id, entry_group):
        created_entry_group = self.__datacatalog.create_entry_group(
            parent=datacatalog_v1.DataCatalogClient.location_path(project_id, location_id),
            entry_group_id=entry_group_id,
            entry_group=entry_group,
            timeout=1200)
        logging.info('Entry Group created: %s', created_entry_group.name)
        return created_entry_group

    def get_tag_template_name(self, tag_template_name=None, location=None):
        if tag_template_name:
            resolved_tag_template_name = tag_template_name
        else:
            if location:
                resolved_location = location
            else:
                resolved_location = DataCatalogHelper.__LOCATION

            resolved_tag_template_name = datacatalog_v1.DataCatalogClient.tag_template_path(
                self.__project_id, resolved_location, DataCatalogHelper.__TAG_TEMPLATE)
        return resolved_tag_template_name

    @classmethod
    def extract_resources_from_template(cls, tag_template_name):
        re_match = re.match(
            r'^projects[/]([_a-zA-Z-\d]+)[/]locations[/]'
            r'([a-zA-Z-\d]+)[/]tagTemplates[/]([@a-zA-Z-_\d]+)$', tag_template_name)

        if re_match:
            project_id, location_id, tag_template_id, = re_match.groups()
            return project_id, location_id, tag_template_id

    @classmethod
    def extract_resources_from_entry_group(cls, entry_group_name):
        re_match = re.match(
            r'^projects[/]([_a-zA-Z-\d]+)[/]locations[/]'
            r'([a-zA-Z-\d]+)[/]entryGroups[/]([@a-zA-Z-_\d]+)$', entry_group_name)

        if re_match:
            project_id, location_id, entry_group_id, = re_match.groups()
            return project_id, location_id, entry_group_id

    def get_entry(self, name):
        return self.__datacatalog.get_entry(name=name)

    def get_entry_group(self, name):
        return self.__datacatalog.get_entry_group(name=name)

    def synchronize_entry(self, entry_group_name, entry_id, entry, tags):
        start_time = timeit.default_timer()
        try:
            self.upsert_entry(entry_group_name, entry_id, entry)
            self.synchronize_tags(entry.name, tags)
            stop_time = timeit.default_timer()
            elapsed_time = int(stop_time - start_time)
            entry_name = entry.name
            logging.info(f'=> Sync Entry: {entry_name} took [{elapsed_time} seconds]')
            return entry_name
        except exceptions.GoogleAPICallError as e:
            logging.warning('Entry was not synchronized: %s', entry_id)
            logging.warning('Error: %s', str(e))

    def synchronize_tags(self, entry_name, tags):
        current_tags = self.__datacatalog.list_tags(parent=entry_name)
        for tag in tags:
            tag_to_create = tag
            tag_to_update = None
            for current_tag in current_tags:
                logging.info(f'Tag loaded: {current_tag.name}')
                if tag.template == current_tag.template:
                    tag_to_create = None
                    if not self.__tags_fields_are_equal(tag, current_tag):
                        tag.name = current_tag.name
                        tag_to_update = tag

            if tag_to_create:
                tag = self.__datacatalog.create_tag(parent=entry_name, tag=tag_to_create)
                logging.info(f'Tag created: {tag.name}')
            elif tag_to_update:
                self.__datacatalog.update_tag(tag=tag_to_update, update_mask=None)
                logging.info(f'Tag updated: {tag_to_update.name}')
            else:
                logging.info('Tag is up to date')

    def upsert_entry(self, entry_group_name, entry_id, entry):
        persisted_entry = entry
        entry_name = '{}/entries/{}'.format(entry_group_name, entry_id)
        persisted_entry.name = entry_name
        try:
            persisted_entry = self.get_entry(name=entry_name)
            self.__log_entry_operation('already exists', entry_name=entry_name)
            if self.__entry_was_updated(persisted_entry, entry):
                persisted_entry = self.update_entry(entry=entry)
            else:
                self.__log_entry_operation('is up-to-date', entry=persisted_entry)
        except exceptions.PermissionDenied:
            self.__log_entry_operation('does not exist', entry_name=entry_name)
            persisted_entry = self.create_entry(entry_group_name=entry_group_name,
                                                entry_id=entry_id,
                                                entry=entry)
        except exceptions.GoogleAPICallError as e:
            logging.warning('Entry was not updated: %s', entry_name)
            logging.warning('Error: %s', str(e))

        return persisted_entry

    def create_entry(self, entry_group_name, entry_id, entry):
        try:
            entry = self.__datacatalog.create_entry(parent=entry_group_name,
                                                    entry_id=entry_id,
                                                    entry=entry)
            self.__log_entry_operation('created', entry=entry)
        except exceptions.PermissionDenied as e:
            entry_name = '{}/entries/{}'.format(entry_group_name, entry_id)
            self.__log_entry_operation('was not created', entry_name=entry_name)
            logging.warning('Error: %s', e)

        return entry

    def update_entry(self, entry):
        entry = self.__datacatalog.update_entry(entry=entry, update_mask=None, timeout=1200)
        self.__log_entry_operation('updated', entry=entry)
        return entry

    def delete_obsolete_metadata(self, new_entries_name, system, entry_group_name):
        logging.info('')
        logging.info('Starting to clean obsolete entries...')

        _, _, entry_group_id = self.extract_resources_from_entry_group(entry_group_name)

        # Delete any pre-existing Entries.
        old_entries_name = \
            self.search_catalog_relative_resource_name(f'system={system} {entry_group_id}')

        logging.info('%s entries that match the search query'
                     ' exist in Data Catalog!', len(old_entries_name))
        logging.info('Looking for entries to be deleted...')

        # Filter out entries from same system but different entry_group
        old_entries_name_same_entry_group = []

        for entry_name in old_entries_name:
            datacatalog_entry_name_pattern = \
                '(?P<entry_group_name>.+?)/entries/(.+?)'
            match = re.match(pattern=datacatalog_entry_name_pattern, string=entry_name)
            current_entry_group_name = match.group('entry_group_name')
            if current_entry_group_name == entry_group_name:
                old_entries_name_same_entry_group.append(entry_name)

        entries_name_pending_deletion = set(old_entries_name_same_entry_group).difference(
            set(new_entries_name))

        logging.info('%s entries will be deleted.', len(entries_name_pending_deletion))

        for entry_name in entries_name_pending_deletion:
            self.delete_entry(entry_name)

        try:
            self.delete_entry_group(entry_group_name)
            logging.info('Entry Group deleted: %s', entry_group_name)
        except exceptions.GoogleAPICallError as e:
            logging.info('Unable to delete Entry Group: %s', entry_group_name)
            logging.debug(str(e))

    def delete_entry(self, name):
        try:
            self.__datacatalog.delete_entry(name=name)
            self.__log_entry_operation('deleted', entry_name=name)
        except Exception as e:
            logging.info('An exception ocurred while attempting to' ' delete Entry: %s', name)
            logging.debug(str(e))

    def delete_entry_group(self, name):
        self.__datacatalog.delete_entry_group(name=name)

    def search_catalog_relative_resource_name(self, query):
        return [result.relative_resource_name for result in self.search_catalog(query=query)]

    def search_catalog(self, query):
        scope = datacatalog_v1.types.SearchCatalogRequest.Scope()
        scope.include_project_ids.append(self.__project_id)

        return [
            result for result in self.__datacatalog.search_catalog(
                scope=scope, query=query, order_by='relevance', page_size=1000)
        ]

    def __sync_entries_from_dataframe(self, dataframe, entry_group_name,
                                      resolved_tag_template_name, execution_time, system):
        start_time = timeit.default_timer()
        entries_names = []
        for row in dataframe.itertuples(index=False):
            entry_id = self.__normalize_entry_id(row.file_name)
            entry = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry({
                'display_name':
                f'{entry_id}',
                'description':
                f'This Entry represents the file {row.file_name} '
                f'on system {row.system}',
                'system':
                row.system,
                'type':
                row.file_type,
                'linked_resource':
                row.linked_resource,
                'time_created':
                row.time_created,
                'time_updated':
                row.time_updated
            })

            tag = datacatalog_v1.types.Tag()
            tag.template = resolved_tag_template_name

            tag.fields['bucket_name'].string_value = row.bucket_name
            tag.fields['file_url'].string_value = row.public_url
            tag.fields['file_name'].string_value = row.file_name
            tag.fields['file_size'].double_value = row.size
            tag.fields['execution_time'].timestamp_value.FromJsonString(execution_time.isoformat())

            entry_name = self.synchronize_entry(entry_group_name, entry_id, entry, [tag])
            entries_names.append(entry_name)

        self.delete_obsolete_metadata(entries_names, system, entry_group_name)

        stop_time = timeit.default_timer()
        elapsed_time = int(stop_time - start_time)
        logging.info(f'=> Sync Entries took [{elapsed_time} seconds]')

    def __load_tag_template(self):
        logging.info('===> Load the Tag Template')
        logging.info('')
        resolved_tag_template_name = self.get_tag_template_name()
        try:
            self.__datacatalog.get_tag_template(resolved_tag_template_name)
        except exceptions.AlreadyExists:
            logging.info(f'Tag Template {resolved_tag_template_name} already exists.')
        except exceptions.PermissionDenied:
            self.create_tag_template(resolved_tag_template_name)
        return resolved_tag_template_name

    def __load_entry_group(self, entry_group_id, entry_group_name, location_id, project_id):
        try:
            self.get_entry_group(entry_group_name)
        except exceptions.GoogleAPICallError:
            logging.info('Entry Group %s does not exist, creating it.', entry_group_name)
            entry_group = datacatalog_entity_factory.DataCatalogEntityFactory.make_entry_group({
                'display_name':
                'Container for object storage entries',
                'description':
                'This Entry Group is used as a container for object storage '
                'entries'
            })
            self.create_entry_group(project_id, location_id, entry_group_id, entry_group)

    @classmethod
    def __entry_was_updated(cls, current_entry, new_entry):
        # Update time comparison allows to verify whether the entry was
        # updated on the source system.
        current_update_time = \
            current_entry.source_system_timestamps.update_time.seconds
        new_update_time = \
            new_entry.source_system_timestamps.update_time.seconds

        updated_time_changed = \
            new_update_time != 0 and current_update_time != new_update_time

        return updated_time_changed or not cls.__entries_are_equal(current_entry, new_entry)

    @classmethod
    def __entries_are_equal(cls, entry_1, entry_2):
        object_1 = utils.ValuesComparableObject()
        object_1.user_specified_system = entry_1.user_specified_system
        object_1.user_specified_type = entry_1.user_specified_type
        object_1.display_name = entry_1.display_name
        object_1.description = entry_1.description
        object_1.linked_resource = entry_1.linked_resource

        object_2 = utils.ValuesComparableObject()
        object_2.user_specified_system = entry_2.user_specified_system
        object_2.user_specified_type = entry_2.user_specified_type
        object_2.display_name = entry_2.display_name
        object_2.description = entry_2.description
        object_2.linked_resource = entry_2.linked_resource

        return object_1 == object_2

    @classmethod
    def __tags_fields_are_equal(cls, tag_1, tag_2):
        for field_id in tag_1.fields:
            tag_1_field = tag_1.fields[field_id]
            tag_2_field = tag_2.fields[field_id]

            values_are_equal = tag_1_field.bool_value == tag_2_field.bool_value
            values_are_equal = values_are_equal and tag_1_field.double_value == \
                tag_2_field.double_value
            values_are_equal = values_are_equal and tag_1_field.string_value == \
                tag_2_field.string_value
            values_are_equal = values_are_equal and tag_1_field.timestamp_value.seconds == \
                tag_2_field.timestamp_value.seconds
            values_are_equal = values_are_equal and tag_1_field.enum_value.display_name == \
                tag_2_field.enum_value.display_name

            if not values_are_equal:
                return False

        return True

    @classmethod
    def __log_entry_operation(cls, description, entry=None, entry_name=None):

        formatted_description = 'Entry {}: '.format(description)
        logging.info('%s%s', formatted_description, entry.name if entry else entry_name)

        if entry:
            logging.info('%s^ [%s] %s', ' ' * len(formatted_description),
                         entry.user_specified_type, entry.linked_resource)

    @classmethod
    def __normalize_entry_id(cls, file_name):
        return file_name.split('.')[0].replace('-', '_')
