from websocket_constants import MESSAGE_VALUE_ACKKNOWLEDGE, MESSAGE_VALUE_ANSWER

from .api_types import ApiResponse, UserRequest
from .permissioned_database import HUMAN_DATABASE as DB
from .read import dyndb_
from .route_attributes import GetAttributes
from .routes import GET_ATTRIBUTES, PUT_ATTRIBUTES


def process_put(username: str, attribute_name: str, parameters):
    """
    Put an item to the database.
    When updating an item, it first has to be downloaded to check the permissions of the
    user. Therefore it's not an update operation, it's a put operation.
    Create does not check whether an item with the same hash key exists, therefore it's
    not a create but instead a put operation.
    :param username: The name of the user who signed in to aws
    :param attribute_name: Key pointing to PutAttributes instance in PUT_ATTRIBUTES
    :param parameters: Parameters given to the api
    :return: Number of uploaded items, status code
    """

    attributes = PUT_ATTRIBUTES[attribute_name]

    def _check_params(params: dict):
        columns = attributes.columns.copy()
        replace = params.pop('replace', None)
        if attributes.replaced_key:
            payload = params.pop('data', None)
            hash_key = list(next(iter(params.items())))
            params = {attributes.replaced_key: payload}
            item = DB.read.primary(username, attributes.table_name, 4, [hash_key])
            if not item:
                return False
            item = item[0]
        else:
            item = {}
        for key, value in params.items():
            column = columns.pop(key, None)
            if column is None or (value is None and not column.optional):
                return False
            itm = {key: value}
            dyndb_(itm)
            if (next(iter(next(iter(itm.values())).keys())) != column.dtype or
                    (column.source is not None and
                     not DB.read.primary(username, column.source[0], 4,
                                         [[column.source[1], value]]))):
                return False
            if not replace and isinstance(value, dict):
                if key not in item:
                    item[key] = {}
                for item_key, item_value in value.items():
                    item[key][item_key] = item_value
            else:
                item[key] = value

        if not attributes.replaced_key:
            for column in columns.values():
                if not column.optional:
                    return False
        dyndb_(item)
        return item

    if isinstance(parameters, dict):
        items = [_check_params(parameters)]
    elif isinstance(parameters, list):
        items = [_check_params(item) for item in parameters]
    else:
        raise UserWarning(f"Unsupported type {type(parameters)} for 'parameters'")
    items = [item for item in items if item]
    if not items:
        return None, 422, MESSAGE_VALUE_ACKKNOWLEDGE
    upload_count = DB.write.put_item(username, attributes.table_name, 2, items)
    return upload_count, 201 if upload_count else 403, MESSAGE_VALUE_ACKKNOWLEDGE


def process_get(username: str, attribute_name: GetAttributes, parameters: dict):
    """
    Process a get request with permission check and ability to filter. Filters have to
    be defined globally by instantiating a GET_REQUESTS class above.
    :param username: The username of the person calling the API
    :param attribute_name: Key pointing to GetAttributes instance in GET_ATTRIBUTES
    :param parameters: Parameters passed to the API
    :return: Response of the database (potentially filtered or censored), status code
    """

    attributes = GET_ATTRIBUTES[attribute_name]

    response = attributes.function(username,
                                   attributes.table_name,
                                   4,
                                   None if attributes.database_parameter_map is None
                                   else [[db_key,
                                          parameters[param_key]
                                          if isinstance(param_key, str)
                                          else parameters.get(param_key[0],
                                                              param_key[1])
                                          ] for param_key, db_key in
                                         attributes.database_parameter_map],
                                   **attributes.extra_args
                                   )
    if response is None:  # None -> There are items, but no access
        return {} if attributes.single_element else [], 403, MESSAGE_VALUE_ANSWER
        # No permission  -> 403 (Forbidden)

    if attributes.output_filter is not None:
        if (isinstance(attributes.output_filter, list) or
                isinstance(attributes.output_filter, tuple)):
            def _apply_filter(item):
                return {key: item.get(key) for key in attributes.output_filter}
        elif isinstance(attributes.output_filter, str):
            def _apply_filter(item):
                return {attributes.output_filter: item[attributes.output_filter]}
        else:
            raise UserWarning(f"Unknown datatype {type(attributes.output_filter)} "
                              f"for output filter.")
        response = [_apply_filter(item) for item in response]
    if attributes.sort_key is not None:
        response = sorted(response, key=lambda x: x[attributes.sort_key])
    if attributes.callback is not None:
        attributes.callback(username, attributes.table_name, response, parameters)
    if attributes.single_element:
        response = response[0] if response else {}
    return response, 200, MESSAGE_VALUE_ANSWER  # Request worked -> 200 (Success)


def process_call(request: UserRequest):
    """
    Process a given call for an already extracted event.
    :param request: UserRequest containing required information to process request
    :return: ApiResponse instance containing response information
    """

    response = ApiResponse(request)

    if request.request_type in GET_ATTRIBUTES:
        _fn = process_get
    elif request.request_type in PUT_ATTRIBUTES:
        _fn = process_put
    else:
        def _fn(*args):
            return None, 404, MESSAGE_VALUE_ANSWER
            # Unknown request/route -> 404  (Not Found)
    (response.content,
     response.status_code,
     response.message_type) = _fn(request.username,
                                  request.request_type,
                                  request.parameters)
    if not response.content and response.status_code == 200:
        response.status_code = 204  # Empty response, but successful -> 204 (No Content)
    return response
