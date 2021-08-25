import click

@click.group()
def waze():
    pass

@waze.command()
def get_data():
    """Get data from Waze and save to DB"""
    from .get_data import get_waze_data
    get_waze_data()
