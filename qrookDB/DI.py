import inject
from data_formatter import *
from IConnector import *
from PostgresConnector import PostgresConnector


def config_DB(binder, connector, data_formatter):
    binder.bind(IDataFormatter, data_formatter())
    binder.bind(IConnector, connector())


def register(format_type, connector_type, *conn_args, **conn_kwargs):
    if connector_type == 'postgres':
        connector = lambda: PostgresConnector(*conn_args, **conn_kwargs)
    else:
        raise Exception('unknown connector type')

    if format_type == 'list':
        data_formatter = ListDataFormatter
    elif format_type in ['dict', None]:
        data_formatter = DictDataFormatter
    else:
        raise Exception('unknown format type')

    inject.clear_and_configure(lambda binder: config_DB(binder, connector, data_formatter))


# default empty configuration. used to avoid import errors on not-configured injections
if not inject.is_configured():
    inject.clear_and_configure(lambda binder: None)