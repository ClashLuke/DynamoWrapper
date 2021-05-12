import os

from .permissioned_database import HUMAN_DATABASE as DB


def resolve_location(username: str,
                     table_name: str,
                     response: list,
                     parameters: dict):
    idx = 0
    for item in response:
        location = DB.read.newest(username,
                                  os.environ['LocationTable'],
                                  4,
                                  ['hash_key', item['location']])
        if location:
            item['location'] = location[0]
            idx += 1
        else:
            del response[idx]


def get_sharing_point_history_callback(username: str,
                                       table_name: str,
                                       response: list,
                                       parameters: dict):
    """
    Callback function for get_sharing_point_XYZ_history requests. Handles everything
    the generic handler couldn't do. In this case it adds an out-of-range value if
    requested.
    :param username:
    :param table_name: Name of DynamoDB table requests will go to
    :param response: Existing response new items will be appended to (-> pointer)
    :param parameters: Parameters that weren't consumed by the generic handler
    :return: None
    """
    if (not parameters or not parameters.get('initial_value') or
            'sp_serial_number' not in parameters or 'start_time' not in parameters):
        return
    sp_serial_number = parameters['sp_serial_number']
    timestamp = parameters['start_time']
    item = DB.read.primary(username,
                           table_name,
                           4,
                           [["sp_serial_number", sp_serial_number],
                            ['timestamp', timestamp]],
                           ['=', '<'],
                           extra_args={'Limit': 1, 'ScanIndexForward': False}
                           )
    if item:
        response.insert(0, item[0])
