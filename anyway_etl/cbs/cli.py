import click


@click.group()
def cbs():
    pass


@cbs.command()
def import_emails():
    """Retrieve cbs emails from imap and save to a datapackage"""
    from . import import_emails
    import_emails.main()


@cbs.command()
@click.option('--limit-rows')
def process_files(**kwargs):
    """Extract and process the cbs files"""
    from . import process_files
    process_files.main(**kwargs)


@cbs.command()
@click.option('--load-start-year')
def import_to_datastore(**kwargs):
    """Import the data from yearly directories to the DB"""
    from . import import_to_datastore
    import_to_datastore.main(**kwargs)
