import datetime

from ..db import session_decorator


def check(session, min_update_dt, table_name):
    dt = list(session.execute('select max(accident_timestamp) from {}'.format(table_name)))[0][0]
    print('{}: {}'.format(table_name, dt))
    if dt < min_update_dt:
        print('ERROR! update is too old')
        return False
    else:
        return True


@session_decorator
def main(session, last_update_ttl_days):
    min_update_dt = datetime.datetime.now() - datetime.timedelta(days=last_update_ttl_days)
    if all((check(session, min_update_dt, table_name)
            for table_name
            in ['involved_markers_hebrew', 'markers_hebrew', 'vehicles_markers_hebrew'])):
        print('OK: all tables updated in last {} days'.format(last_update_ttl_days))
        return True
    else:
        print('ERROR: some tables were not updated in last {} days'.format(last_update_ttl_days))
        return False
