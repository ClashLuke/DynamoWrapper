import jsonpickle

from .permissions import STAGE_PERMISSIONS as PERMS
from .read import QUERY_INSTANCE as DB, undyndb_


def _perm_wrapper_wrapper(undyndb, post_check):
    def _perm_wrapper(function):
        def _get_perms(client_id, table_name, target_permission, items):
            if isinstance(items, dict) and 'Items' in items:
                items = items['Items']
            if isinstance(items, str):
                return _get_perms(client_id,
                                  table_name,
                                  target_permission,
                                  jsonpickle.loads(items))
            if undyndb:
                undyndb_(items)

            def _item_perms(item):
                return PERMS.get_item_permissions(table_name,
                                                  jsonpickle.dumps(item),
                                                  client_id
                                                  ) & target_permission

            if isinstance(items, list):
                new_items = list(filter(_item_perms, items))
                return None if not new_items and len(items) else new_items
            if isinstance(items, dict):
                return items if _item_perms(items) else None, 1
            raise UserWarning(f"Unsupported type {type(items)}")

        if post_check:
            def _wrapped_function(client_id, table_name, target_permission, *args,
                                  **kwargs):
                items = function(table_name, *args, **kwargs)
                return _get_perms(client_id, table_name, target_permission, items)
        else:
            def _wrapped_function(client_id, table_name, target_permission, user_items):
                allowed_items = _get_perms(client_id,
                                           table_name,
                                           target_permission,
                                           user_items)
                if not allowed_items:
                    return 0

                for item in allowed_items:
                    function(table_name, item)
                return len(allowed_items)

        return _wrapped_function

    return _perm_wrapper


class _Clone:
    def __init__(self, module, undyndb, post_check):
        function_names = list(filter(lambda attr: not attr.startswith('_'),
                                     dir(module)))
        any(map(lambda *x: setattr(self, *x),
                function_names,
                map(_perm_wrapper_wrapper(undyndb, post_check),
                    map(lambda x: getattr(module, x),
                        function_names))))

        # Get the function behind the name from the list above. Feed that into the
        # permission wrapper above, to get a new, permissioned function. Set this
        # function as a new member of the current instance of the class, using the name
        # it had in the other class. Now repeat that for all functions, until setattr
        # won't return None (Hint: it returns none no matter what).


class PermissionedDatabase:
    """
    Simple and fast interface for a permissioned database.
    The initialization is pretty optimized, general and abstract. This allows seamless
    addition and integration of new methods, just by adding them to the wrapped class.
    The heavy-lifting is done by the dynamo wrapper and the permission library
    respectively, with the latter being cached, to optimize the request speed as much as
    possible.
    """

    def __init__(self, undyndb):
        self.read = _Clone(DB.read, undyndb, True)
        self.write = _Clone(DB.write, False, False)


HUMAN_DATABASE = PermissionedDatabase(True)
