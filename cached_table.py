from .read import QUERY_INSTANCE as DB, dyndb_, undyndb_


class CachedTable:
    """
    A table that caches all items retrieved from the database in a simple dictionary.
    If a key exists in the cache, it won't be queried again, unless the state of the
    function gets erased.
    """

    def __init__(self, name, primary_key, projected_secondary_keys=None):
        self.name = name
        self.primary_key = primary_key
        self.primary_cache = {}
        self.secondary_cache = {}
        if isinstance(projected_secondary_keys, list):
            for key in projected_secondary_keys:
                self.secondary_cache[key] = {}

    def primary(self, value, transform=None):
        """
        Query the previously defined table's primary key by value. Composite keys are
        not supported.
        :param value: Value the database will look for in the primary key.
        :return: Item found in cache or database
        """
        if value in self.primary_cache:
            return self.primary_cache[value]
        items = DB.read.primary(self.name, [[self.primary_key, value]])['Items']
        undyndb_(items)
        if transform is not None:
            items = transform(items)
        self.primary_cache[value] = items
        return items

    def secondary(self, key, value, insert_const=False):
        """
        Query the previously defined table's secondary key by value of the secondary
        key. Some databases have a "const" value as hash key for the secondary key,
        to avoid issues, it can be inserted using the appropiate flag.
        :param key: The name of the secondary key
        :param value: The value for the secondary key the database will look for
        :param insert_const: Whether to use the value as sort key (True)
        :return: Item found in cache or database
        """
        if key in self.secondary_cache:
            cache = self.secondary_cache[key]
            if value in cache:
                return cache[value]
        else:
            cache = self.secondary_cache[key] = {}
        items = DB.read.secondary(self.name, [[key, value]], index_name=key,
                                  insert_const=insert_const)['Items']
        undyndb_(items)
        cache[value] = items
        return items

    def write(self, item: dict):
        """
        Write a given item to the database and the cache.
        :param item: Dictionary in non-dynamodb format.
        :return: None
        """
        self.primary_cache[item[self.primary_key]] = item
        item = item.copy()
        dyndb_(item)
        DB.write.put_item(self.name, item)
