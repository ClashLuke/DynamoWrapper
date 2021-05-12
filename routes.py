from .permissioned_database import HUMAN_DATABASE as DB
from .route_attributes import DatabaseColumn, GetAttributes, PutAttributes
from .specific_functions import get_sharing_point_history_callback, resolve_location

PUT_ATTRIBUTES = {'putSharingPointSettings':
                      PutAttributes('SharingPointDataTable',
                                    {'settings': DatabaseColumn('M', False)},
                                    'sp_serial_number',
                                    'settings'),
                  'putSharingPoint':
                      PutAttributes('SharingPointDataTable',
                                    {'location':
                                         DatabaseColumn('N', False,
                                                        ('LocationTable',
                                                         'hash_key')),
                                     'manufacturer':
                                         DatabaseColumn('N', False,
                                                        ('ManufacturerTable',
                                                         'hash_key')),
                                     'owner':
                                         DatabaseColumn('N', False, ('OwnerTable',
                                                                     'hash_key')),
                                     'provider':
                                         DatabaseColumn('N', False, ('ProviderTable',
                                                                     'hash_key')),
                                     'region':
                                         DatabaseColumn('N', False, ('RegionTable',
                                                                     'hash_key')),
                                     'sharing_point_type':
                                         DatabaseColumn('S', False),
                                     'sp_serial_number':
                                         DatabaseColumn('S', False),
                                     'settings':
                                         DatabaseColumn('M', False),
                                     'validation_hash_key':
                                         DatabaseColumn('S', False),
                                     'visibility':
                                         DatabaseColumn('N', False)
                                     },
                                    'sp_serial_number')
                  }

GET_ATTRIBUTES = {
        'getSharingPointSettings':
            GetAttributes('SharingPointDataTable',
                          'sp_serial_number',
                          ('settings', 'sharing_point_type'),
                          single_element=True),
        'getSwapOrders':
            GetAttributes('SwapOrderTable',
                          [('sp_serial_number', 'sp_serial_number'),
                           (('start_time', 0), 'timestamp'),
                           (('end_time', 2 ** 32 * 1000), 'timestamp')
                           ],
                          sort_key='timestamp',
                          function=DB.read.secondary,
                          extra_args={'index_name': 'sp_serial_number_timestamp',
                                      'insert_const': False
                                      }),
        'getSwapActions':
            GetAttributes('SwapActionTable',
                          [('sp_serial_number', 'sp_serial_number'),
                           (('start_time', 0), 'timestamp'),
                           (('end_time', 2 ** 32 * 1000), 'timestamp')
                           ],
                          function=DB.read.secondary,
                          sort_key='timestamp',
                          extra_args={'index_name': 'sp_serial_number_timestamp',
                                      'insert_const': False
                                      }),
        'getSharingPointBoxes':
            GetAttributes('SharingPointBoxTable',
                          'sp_serial_number'),
        'getSharingPointStatus':
            GetAttributes('SharingPointStatusTable',
                          'sp_serial_number'),

        'getSharingPointTemperatureHistory':
            GetAttributes('SharingPointTempHistTable',
                          [('sp_serial_number', 'sp_serial_number'),
                           (('start_time', 0), 'timestamp'),
                           (('end_time', 2 ** 32 * 1000), 'timestamp')
                           ],
                          sort_key='timestamp',
                          callback=get_sharing_point_history_callback),

        'getSharingPointGsmHistory':
            GetAttributes('SharingPointGSMHistTable',
                          [('sp_serial_number', 'sp_serial_number'),
                           (('start_time', 0), 'timestamp'),
                           (('end_time', 2 ** 32 * 1000), 'timestamp')
                           ],
                          sort_key='timestamp',
                          callback=get_sharing_point_history_callback),

        'getSharingPointOnlineHistory':
            GetAttributes('SharingPointOnlineHistTable',
                          [('sp_serial_number', 'sp_serial_number'),
                           (('start_time', 0), 'timestamp'),
                           (('end_time', 2 ** 32 * 1000), 'timestamp')
                           ],
                          sort_key='timestamp',
                          callback=get_sharing_point_history_callback),
        'getSharingPointTableData':
            GetAttributes('SharingPointTable',
                          None,
                          function=DB.read.all,
                          extra_args={'index': 'sp_serial_number'}),
        'getSharingPointMapData':
            GetAttributes('SharingPointTable',
                          None,
                          output_filter=['sp_serial_number', 'location'],
                          function=DB.read.all,
                          extra_args={'index': 'sp_serial_number'},
                          callback=resolve_location),
        }
