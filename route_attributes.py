import os

from .permissioned_database import HUMAN_DATABASE as DB


class Attributes:
    """
    A basic store for attributes related to the function that is being called.
    Instances of this class are used instead of writing new functions every time.
    """

    def __init__(self, table_name, function, callback, extra_args):
        self.table_name = os.environ[table_name]
        self.function = function
        self.callback = callback
        self.extra_args = extra_args


class GetAttributes(Attributes):
    """
    A more specific storage of attributes related to the get handlers
    """

    def __init__(self,
                 table_name,
                 database_parameter_map,
                 output_filter=None,
                 function=DB.read.primary,
                 sort_key=None,
                 callback=None,
                 extra_args=None,
                 single_element=False):
        if extra_args is None:
            extra_args = {}
        if isinstance(database_parameter_map, str):
            database_parameter_map = ((database_parameter_map, database_parameter_map),)
        if isinstance(output_filter, str):
            database_parameter_map = (output_filter,)
        self.database_parameter_map = database_parameter_map
        self.output_filter = output_filter
        self.sort_key = sort_key
        self.single_element = single_element
        super().__init__(table_name, function, callback, extra_args)


class DatabaseColumn:
    """
    Defined a collumn with its types in the database. Used for type safety.
    """

    def __init__(self, dtype, optional=False, source=None):
        self.dtype = dtype
        self.optional = optional
        self.source = None if source is None else (os.environ[source[0]], source[1])


class PutAttributes(Attributes):
    """
    A more specific storage of attributes related to the create/update handlers
    """

    def __init__(self,
                 table_name,
                 columns,
                 hash_key,
                 replaced_key=None,
                 function=DB.write.put_item,
                 callback=None,
                 extra_args=None):
        self.columns = columns
        self.replaced_key = replaced_key
        self.hash_key = hash_key
        super().__init__(table_name, function, callback, extra_args)
