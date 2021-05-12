from typing import Union

import boto3
import jsonpickle
from boto3.dynamodb.conditions import Key as Boto3Key


def undyndb_dict(indict):
    if isinstance(indict, dict):
        indict = list(indict.keys())[0]
    return indict


def list_to_dyndb(inlist):
    #    [['timestamp', 100],['timestamp', 200],['sp_serial_number', 'SP20M']]
    out = {}
    for i, (k, v) in enumerate(inlist):
        v = undyndb_dict(v)
        out[chr(i) + k] = v
    dyndb_(out)
    out = list(out.items())
    out = [[o[0][1:], o[1]] for o in out]
    return out


def items_to_vn(items):  # Values, Names
    values = {}
    names = {}
    prev_k = None
    for i, (k, v) in enumerate(items):
        values[f':v{i}'] = v
        if k != prev_k:
            names[f'#k{i}'] = k
            prev_k = k
    return values, names


class Query:
    def __init__(self, client):
        self._client = client

    def newest(self, table_name, key, index_name=None):
        if index_name is None:
            return self.primary(table_name,
                                [key, ['timestamp', 0]],
                                ['=', '>'],
                                extra_args={'ScanIndexForward': False,
                                            'Limit': 1
                                            })['Items']
        else:

            return self.secondary(table_name,
                                  [key, ['timestamp', 0]],
                                  conditions=['=', '>'],
                                  index_name=index_name,
                                  insert_const=False,
                                  extra_args={'ScanIndexForward': False,
                                              'Limit': 1
                                              })['Items']

    def primary(self, table_name, keys, conditions: Union[list, str] = '=',
                extra_args=None, **kwargs):
        if isinstance(conditions, str):
            conditions = [conditions]
        return self.keys(table_name, keys, None, conditions, extra_args=extra_args)

    def secondary(self, table_name, keys, index_name=None,
                  conditions: Union[list, str] = '=', insert_const=True,
                  extra_args=None, **kwargs):
        if insert_const:
            keys.insert(0, ['const', 1])
        if index_name is None:
            index_name = keys[1][0]
        if isinstance(conditions, list):
            if len(conditions) == 1:
                conditions.insert(0, '=')
        elif isinstance(conditions, str):
            conditions = ["=", conditions]
        return self.keys(table_name, keys, index_name, conditions,
                         extra_args=extra_args)

    def keys(self, table_name, keys, index_name=None, conditions=None, extra_args=None,
             **kwargs):
        key_count = len(keys)
        if conditions is None:
            conditions = ["="] * (1 + int(key_count > 1))
        if not isinstance(conditions, list):
            conditions = [conditions]
        k = f"#k0 {conditions[0]} :v0"
        if isinstance(keys, dict):
            keys = [list(k) for k in keys.items()]
        for i in range(key_count):
            keys[i][1] = undyndb_dict(keys[i][1])
        if key_count == 2:
            k += f' AND #k1 {conditions[1]} :v1'
        elif key_count == 3:
            k += f' AND #k1 BETWEEN :v1 AND :v2'
            # There is no option other than BETWEEN for three keys.
        keyitems = list_to_dyndb(keys)
        v, n = items_to_vn(keyitems)
        params = dict(TableName=table_name, IndexName=index_name,
                      ExpressionAttributeValues=v, KeyConditionExpression=k,
                      ExpressionAttributeNames=n)
        if extra_args is not None:
            params.update(extra_args)
        params = {k: v for k, v in params.items() if k is not None and v is not None}
        ret = self._client.query(**params)
        return ret

    def chunk(self,
              table_name,
              start_key=None,
              condition=None,
              key=None,
              index=None,
              **kwargs):
        params = {'TableName': table_name}
        if index is not None:
            params['IndexName'] = index
        if start_key is not None:
            params['ExclusiveStartKey'] = start_key
        if condition is not None and key is not None:
            ((k, v),) = key.items()
            if isinstance(v, dict):
                v = list(v.keys())[0]
            params['KeyConditionExpression'] = getattr(Boto3Key(k), condition)(v)
        return self._client.scan(**params)

    def all(self, table_name, condition=None, key=None, index=None, **kwargs):
        start_key = None
        return_items = []
        while start_key is not False:
            request = self.chunk(table_name, start_key, condition, key, index)
            return_items.extend(request.get('Items', []))
            start_key = request.get('LastEvaluatedKey', False)
        return return_items


class Put:
    def __init__(self, client):
        self._client = client

    def put_item(self, table_name, item):
        self._client.put_item(TableName=table_name, Item=item)

    def update_item(self, table_name, item):
        self._client.update_item(TableName=table_name, Key=item)


class Database:
    def __init__(self):
        self._client = boto3.client('dynamodb')
        self.read = Query(self._client)
        self.write = Put(self._client)


QUERY_INSTANCE = Database()

types = {str: 'S',
         int: 'N',
         float: 'N',
         dict: 'M',
         list: 'L',
         tuple: 'L',
         type(None): 'NULL',
         bool: 'BOOL'
         }
conversions = {value: key for key, value in types.items()}


def _undyndb_(indict):  # prefix _ indicates hidden/private function
    for key, value in indict.items():
        dtype, data = value.popitem()
        indict[key] = conversions[dtype](data)


def decode_number(number_string):
    if '.' in number_string:
        return float(number_string)
    return int(number_string)


conversions['N'] = decode_number
conversions['NULL'] = lambda x: None


def undyndb_(fn_input):  # trailing _ indicates inplace operation
    if not fn_input:
        return
    if isinstance(fn_input, list):
        if isinstance(next(iter(fn_input[0].values())), dict):
            def _handler(idx, item):
                _undyndb_(item)
        else:
            def _handler(idx, item):
                dtype, data = item.popitem()
                fn_input[idx] = conversions[dtype](data)
        for idx, item in enumerate(fn_input):
            _handler(idx, item)
        return
    if isinstance(fn_input, dict):
        return _undyndb_(fn_input)
    if isinstance(fn_input, str):
        decoded_input = jsonpickle.decode(fn_input)
        undyndb_(decoded_input)
        return decoded_input


def undyndb(fn_input: dict):
    fn_input = fn_input.copy()
    undyndb_(fn_input)
    return fn_input


conversions['M'] = undyndb
conversions['L'] = undyndb
encode_conversions = {'N': str,
                      'S': lambda x: x,
                      'BOOL': lambda x: x
                      }


def dyndb_(in_dict: dict):
    if isinstance(in_dict, dict):
        iterator = in_dict.items()
    elif isinstance(in_dict, list):
        iterator = enumerate(in_dict)
    for key, value in iterator:
        dtype = types[value.__class__]
        in_dict[key] = {dtype: encode_conversions[dtype](value)}


def dyndb(in_dict: dict):
    if isinstance(in_dict, dict) or isinstance(in_dict, list):
        in_dict = in_dict.copy()
    elif isinstance(in_dict, tuple):
        in_dict = list(in_dict)
    dyndb_(in_dict)
    return in_dict


encode_conversions['M'] = dyndb
encode_conversions['L'] = dyndb
encode_conversions['NULL'] = lambda x: None


def list_wrapper_(in_list, fn):
    for itm in in_list:
        fn(itm)
