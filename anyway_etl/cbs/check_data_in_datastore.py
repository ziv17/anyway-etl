import datetime

from ..db import session_decorator


def check(session, min_update_dt, tables_names):
    df_tables = {}
    for table_name in tables_names:
        dt = list(session.execute('select max(accident_timestamp) from {}'.format(table_name)))[0][0]
        df_tables[table_name] = dt
        print('{}: {}'.format(table_name, dt))
        if dt < min_update_dt:
            print('ERROR! update is too old')
            return False
    return len(set(df_tables.values())) == 1


@session_decorator
def main(session, last_update_ttl_days):
    min_update_dt = datetime.datetime.now() - datetime.timedelta(days=last_update_ttl_days)
    tables_names = ['involved_markers_hebrew', 'markers_hebrew', 'vehicles_markers_hebrew']
    if check(session, min_update_dt, tables_names):
        print('OK: all tables updated in last {} days'.format(last_update_ttl_days))
        return True
    else:
        print('ERROR: some tables were not updated in last {} days or tables do not have the same max timestamp'.format(last_update_ttl_days))
        return False
