from google.cloud import datacatalog_v1


class DataCatalogEntityFactory:

    @classmethod
    def make_entry_group(cls, entry_group_dict):
        entry_group = datacatalog_v1.types.EntryGroup()
        entry_group.display_name = entry_group_dict['display_name']
        entry_group.description = entry_group_dict['description']

        return entry_group

    @classmethod
    def make_entry(cls, entry_dict):
        entry = datacatalog_v1.types.Entry()

        entry.user_specified_system = entry_dict['system']
        entry.user_specified_type = entry_dict['type']
        entry.display_name = entry_dict['display_name']
        entry.description = entry_dict['description']
        entry.linked_resource = entry_dict['linked_resource']
        entry.source_system_timestamps.create_time.seconds = int(
            entry_dict['time_created'].timestamp())
        entry.source_system_timestamps.update_time.seconds = int(
            entry_dict['time_updated'].timestamp())

        return entry
