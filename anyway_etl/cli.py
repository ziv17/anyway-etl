import click

from .cbs.cli import cbs
from .waze.cli import waze
from . import anyway_kubectl


@click.group(context_settings={'max_content_width': 200})
def main():
    """Anyway ETL"""
    pass


main.add_command(cbs)
main.add_command(waze)


@main.command(context_settings={"allow_interspersed_args": False})
@click.argument('ARGS', nargs=-1)
def anyway_kubectl_exec(args):
    anyway_kubectl.check_call(*args)
