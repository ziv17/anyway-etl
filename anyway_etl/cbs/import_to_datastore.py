import datetime

from sqlalchemy import or_

from anyway.models import AccidentMarker, Involved, Vehicle

from ..db import session_decorator


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


@session_decorator
def delete_cbs_entries(session, start_year, batch_size=5000):
    """
    deletes all CBS markers (provider_code=1 or provider_code=3) in the database created in year and with provider code provider_code
    first deletes from tables Involved and Vehicle, then from table AccidentMarker
    """
    print("Deleting old cbs entries")
    start_date = f"{start_year}-01-01"
    marker_ids_to_delete = (
        session.query(AccidentMarker.id)
        .filter(AccidentMarker.created >= datetime.datetime.strptime(start_date, "%Y-%m-%d"))
        .filter(
            or_(
                (AccidentMarker.provider_code == 1),
                (AccidentMarker.provider_code == 3),
            )
        )
        .all()
    )

    marker_ids_to_delete = [acc_id[0] for acc_id in marker_ids_to_delete]

    print(
        "There are "
        + str(len(marker_ids_to_delete))
        + " accident ids to delete starting "
        + str(start_date)
    )

    for ids_chunk in chunks(marker_ids_to_delete, batch_size):

        print("Deleting a chunk of " + str(len(ids_chunk)))

        q = session.query(Involved).filter(Involved.accident_id.in_(ids_chunk))
        if q.all():
            print("deleting entries from Involved")
            q.delete(synchronize_session=False)
            session.commit()

        q = session.query(Vehicle).filter(Vehicle.accident_id.in_(ids_chunk))
        if q.all():
            print("deleting entries from Vehicle")
            q.delete(synchronize_session=False)
            session.commit()

        q = session.query(AccidentMarker).filter(AccidentMarker.id.in_(ids_chunk))
        if q.all():
            print("deleting entries from AccidentMarker")
            q.delete(synchronize_session=False)
            session.commit()


def main(load_start_year=None):
    load_start_year = int(load_start_year) if load_start_year else datetime.datetime.now().year - 1
    print("Importing to datastore (load_start_year={})".format(load_start_year))
    delete_cbs_entries(load_start_year)
