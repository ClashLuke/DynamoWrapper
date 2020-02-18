import boto3
from boto3.dynamodb.conditions import Key as Boto3Key
from dynamodb_json.json_util import dumps


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
    out = dumps(out, as_dict=True)
    out = list(out.items())
    out = [[o[0][1:], o[1]] for o in out]
    return out


def items_to_vn(items):  # Values, Names
    values = {}
    names = {}
    print("itm:", items)
    prev_k = None
    for i, (k, v) in enumerate(items):
        values[f':v{i}'] = v
        if k != prev_k:
            names[f'#k{i}'] = k
            prev_k = k
    print(values, names)
    return values, names


class Query:
    # ToDo: Comfort functions

    def __init__(self):
        self.client = boto3.client('dynamodb')

    def primary(self, table_name, keys, conditions='=', **kwargs):
        if isinstance(conditions, str):
            conditions = [conditions]
        return self.keys(table_name, keys, None, conditions)

    def secondary(self, table_name, keys, index_name=None, conditions='=', **kwargs):
        keys.insert(0, ['const', 1])
        if index_name is None:
            index_name = keys[1][0]
        if isinstance(conditions, list):
            if len(conditions) == 1:
                conditions.insert(0, '=')
        elif isinstance(conditions, str):
            conditions = ["=", conditions]
        return self.keys(table_name, keys, index_name, conditions)

    def keys(self, table_name, keys, index_name=None, conditions=None, **kwargs):
        key_count = len(keys)
        if conditions is None:
            conditions = ["="] * (1 + int(key_count > 1))
        if not isinstance(conditions, list):
            conditions = [conditions]
        k = f"#k0 {conditions[0]} :v0"
        for i in range(key_count):
            keys[i][1] = undyndb_dict(keys[i][1])
        if key_count > 1:
            k = ''.join([k, ' AND #k1 ', conditions[1], ' AND'.join([f' :v{i}' for i in range(1, key_count)])])
        keyitems = list_to_dyndb(keys)
        v, n = items_to_vn(keyitems)
        params = dict(TableName=table_name, IndexName=index_name, ExpressionAttributeValues=v, KeyConditionExpression=k,
                      ExpressionAttributeNames=n)
        params = {k: v for k, v in params.items() if k is not None and v is not None}
        print(params)
        ret = self.client.query(**params)
        return ret

    def chunk(self, table_name, start_key=None, condition=None, key=None, **kwargs):
        params = dict(TableName=table_name)
        if start_key is not None:
            params['ExclusiveStartKey'] = start_key
        if condition is not None and key is not None:
            ((k, v),) = key.items()
            if isinstance(v, dict):
                v = list(v.keys())[0]
            params['KeyConditionExpression'] = getattr(Boto3Key(k), condition)(v)
        return self.client.scan(**params)

    def all(self, table_name, condition=None, key=None, **kwargs):
        start_key = None
        return_items = []
        while start_key is not False:
            request = self.chunk(table_name, start_key, condition, key)
            return_items.extend(request.get('Items', []))
            start_key = request.get('LastEvaluatedKey', False)
        return return_items

    def test(self):
        from time import time
        table_name = 'database-luc-SharingPointTempHistTable'
        sp = [['sp_serial_number', 'SP 20M 0009']]
        ts = [['timestamp', 1578867321712]]
        start_time = time()
        p = self.primary(table_name, sp)
        s = self.secondary(table_name, ts, condition='>')
        k = self.keys(table_name, sp + [['timestamp', 1578567321712]], None, ['=', ">"])
        end_time = time()
        assert s['Count'] == 13
        assert p['Count'] == 102
        assert k['Count'] == 62
        assert s['Count'] == s['ScannedCount']
        assert p['Count'] == p['ScannedCount']
        assert k['Count'] == k['ScannedCount']
        return end_time - start_time
