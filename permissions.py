import hashlib
import os
import re

from .cached_table import CachedTable

READ_ACCESS = 4
WRITE_ACCESS = 2


def get_hash(data: str):
    return hashlib.blake2b(data.encode()).hexdigest()


def compile(permission_list: list):
    print(permission_list)
    return [{key: re.compile(value) if isinstance(value, str) else value
             for key, value in permissions.items()}
            for permissions in permission_list]


class User:
    def __init__(self):
        self.group_decoding_table = CachedTable(os.environ['GroupDecodingTable'],
                                                'group_hash')
        self.user_data_table = CachedTable(os.environ['UserDataTable'], 'user_id')
        self.permission_cache = {}
        self.group_hashes = {}
        self.user_hashes = {}

    def settings(self, user_id):
        if user_id is not None and len(user_id) != 128:
            user_id = self.hash_user(user_id)
        user_dict = self.user_data_table.primary(user_id)[0]
        user_dict = {key: value for key, value in user_dict.items() if
                     key.endswith("_setting")}
        return user_dict

    def provider(self, user_id):
        dat = self.user_data_table.primary(user_id)
        if not dat:
            return ""
        return dat[0]['provider']

    def group(self, user_id):
        dat = self.user_data_table.primary(user_id)
        if not dat:
            return ""
        return self.user_data_table.primary(user_id)[0]['group']

    def group_permissions(self, group_hash, provider=False):
        """Get permissions of a given group"""
        perms = self.group_decoding_table.primary(group_hash,
                                                  compile if provider else None)
        if not perms:
            print(f"Didnt find {group_hash}")
            return {}
        return perms[0]

    def hash_group(self, group_name: str):
        if group_name in self.group_hashes:
            return self.group_hashes[group_name]
        dat = get_hash(group_name)
        self.group_hashes[group_name] = dat
        return dat

    def hash_user(self, group_name: str):
        if group_name in self.user_hashes:
            return self.user_hashes[group_name]
        dat = get_hash(group_name)
        self.user_hashes[group_name] = dat
        return dat

    def permissions(self, user_id=None, connection_id=None):
        if len(user_id) != 128:
            user_id = self.hash_user(user_id)

        group = self.group(user_id)
        provider = self.provider(user_id)
        provider_items = self.group_permissions(self.hash_group(provider), True)
        permissions = self.group_permissions(self.hash_group(group))
        return permissions, provider_items

    def get_mode(self, user_id=None, table_name=None, perms=None):
        if perms is None:
            if user_id is not None and len(user_id) != 128:
                user_id = self.hash_user(user_id)
            perms = self.permissions(user_id)
        return int(perms.get(table_name, 0))

    def check_mode(self, user_id, table_name=None, mode=None):
        if user_id is not None and len(user_id) != 128:
            user_id = self.hash_user(user_id)
        return self.get_mode(user_id, table_name) == mode

    def get_item_permissions(self, table_name, item_str="", user_id=None,
                             connection_id=None):
        if user_id is not None and len(user_id) != 128:
            user_id = self.hash_user(user_id)
        permissions, provider_items = self.permissions(user_id, connection_id)
        permissions = self.get_mode(None, table_name, permissions)
        if (not provider_items or
                table_name not in provider_items or
                provider_items[table_name].search(item_str) is None):
            return 0
        return permissions


STAGE_PERMISSIONS = User()
