import click


@click.group()
def waze():
    pass


@waze.command()
def get_data():
    """Get data from Waze"""
    from .get_data import get_waze_data

    get_waze_data()


@waze.command()
def import_to_db():
    """Import local CSV files to DB"""
    from .import_to_db import import_waze_data_to_db

    import_waze_data_to_db()
