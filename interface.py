from .read import Query

to_get = {'dtype':         ['data_type', ''],
          # Ex: TechCV, Battery, SharingPoint
          'ttype':         ['table_type', ''],
          # '', Hist
          'request':       ['request_group', 'Read'],
          # Create, Read, Update
          'access':        ['access_type', 'All'],
          # [item, validated_item], [using_gsi, using_primary, chunk, all], [item,
          # validated_item]
          'segments':      ['threads', 1],
          # N° of threads used in fan-out/in
          # TODO: -1 option to use the maximum number of threads
          'keys':          ['condition_keys', []],
          # Item-Level Filter/Condition KEYS given to DynamoDB
          'conditions':    ['condition_list', ['EQ']],
          # List of database conditions, such as EQ, BETWEEN or GT
          'attributes':    ['attributes', []],
          # Attribute-Level Filter to return only specific rows
          'condition_map': ['condition_map', ''],
          # Predefined map to overwrite given conditions
          'attribute_map': ['attribute_map', ''],
          # Predefined map to overwrite given attributes
          'payload':       ['payload', {}]
          # Ex: Item which is supposed to be updated
          }

request_classes = {'Read': Query}

attribute_maps = {}
condition_maps = {}


def overwrite_variable(args, default_map, arg_index, map_index):
    """
    Pops argument at arg_index from one dictionary (args)
    and sets the value at map_index in args to default_map[argument].
    If the args[arg_index] does not exist in default_map, a declarative exception is
    raised.
    :param args: Dictionary at which in-place operation is performed
    :param default_map: Source dict of map
    :param arg_index: Index of value used to index default_map
    :param map_index: Index the output is set to
    :return: None
    """
    if args.get(arg_index):
        arg = args.pop(arg_index)
        try:
            map = default_map[arg]
        except KeyError:
            raise ValueError(f"{arg} is not supported as {arg_index}")
        args[map_index] = map
    return None


def get_table_name(stage, table_specifier):
    if stage is None:
        return table_specifier
    else:
        return f'database-{stage}-{table_specifier}Table'


def extract_args(stage, **kwargs):
    args = kwargs
    for internal, (api_key, default) in to_get.items():
        args[internal] = type(default)(kwargs.get(api_key, default))

    args['parent'] = request_classes[args['request']]

    if 'table_name' not in args:
        if 'dtype' not in args:
            raise ValueError(f"{to_get['dtype'][0]} not given in request")

        args['table_name'] = get_table_name(stage, ''.join([args['dtype'],
                                                            args['ttype']]))

    overwrite_variable(args, attribute_maps, 'attribute_map', 'attributes')
    overwrite_variable(args, condition_maps, 'condition_map', 'conditions')

    return args


def get_permissions(connection_id=None, user_id=None):
    return None


def init(stage, **kwargs):
    processed_args = extract_args(stage, **kwargs)
    parent = processed_args['parent']()
    return getattr(parent, processed_args['access'])(**processed_args)
