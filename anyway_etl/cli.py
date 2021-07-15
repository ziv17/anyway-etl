import click

from .cbs.cli import cbs


@click.group(context_settings={'max_content_width': 200})
def main():
    """Anyway ETL"""
    pass


main.add_command(cbs)
