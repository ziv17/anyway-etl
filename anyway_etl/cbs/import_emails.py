import os
import email
import imaplib
import datetime
from pprint import pprint
from collections import defaultdict
from contextlib import contextmanager

import dataflows as DF

from .config import (
    IMAP_MAIL_DIR, IMAP_MAIL_USER, IMAP_MAIL_PASSWORD, IMAP_MAIL_HOST,
    CBS_FILES_ROOT_PATH, CBS_EMAILS_DATA_ROOT_PATH
)


@contextmanager
def get_imap_session():
    print('Initializing imap session...')
    assert IMAP_MAIL_USER and IMAP_MAIL_PASSWORD, 'missing required env vars: IMAP_MAIL_USER, IMAP_MAIL_PASSWORD'
    imap_session = imaplib.IMAP4_SSL(IMAP_MAIL_HOST)
    imap_session.login(IMAP_MAIL_USER, IMAP_MAIL_PASSWORD)
    try:
        imap_session.select(IMAP_MAIL_DIR)
        yield imap_session
    finally:
        imap_session.close()
        imap_session.logout()


def imap_search_all(imap_session):
    print('Search imap messages...')
    typ, data = imap_session.search(None, "ALL")
    assert typ == 'OK' and len(data) == 1, 'invalid response from imap search command typ={} data={}'.format(typ, data)
    return data[0].split()


def imap_fetch_message(imap_session, msgId):
    typ, data = imap_session.fetch(msgId, "(RFC822)")
    assert typ == 'OK', 'invalid response from imap fetch command typ={}'.format(typ)
    assert len(data) > 0, 'invalid response form imap fetch command (invalid data length: {})'.format(len(data))
    assert len(data[0]) > 1, 'invalid response from imap fetch command invalid data[0] length: {}'.format(len(data[0]))
    return data[0][1]


def process_email_body(stats, msgId, email_body):
    if type(email_body) is bytes:
        email_body = email_body.decode('utf-8')
    mail = email.message_from_string(email_body)
    try:
        mtime = datetime.datetime.strptime(mail["Date"][:-6], "%a, %d %b %Y %H:%M:%S")
    except ValueError:
        mtime = datetime.datetime.strptime(mail["Date"][:-12], "%a, %d %b %Y %H:%M:%S")
    for part in mail.walk():
        if part.get_content_maintype() == "multipart" or part.get("Content-Disposition") is None:
            stats['invalid_content_type'] += 1
            continue
        filename = part.get_filename()
        if not bool(filename) or not filename.endswith(".zip"):
            stats['invalid_filename'] += 1
            continue
        filename = mtime.strftime('%Y/%m/%d_%H_%M_%S.zip')
        filepath = os.path.join(CBS_FILES_ROOT_PATH, filename)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "wb") as fp:
            fp.write(part.get_payload(decode=True))
        yield {'msgId': msgId, 'mtime': mtime, 'filename': filename, 'filesize': os.path.getsize(filepath)}
        stats['valid_files'] += 1


def process_msgIds(stats, imap_session, msgIds):
    print('Processing {} messages..'.format(len(msgIds)))
    for msgId in msgIds:
        stats['msgIds'] += 1
        email_body = imap_fetch_message(imap_session, msgId)
        yield from process_email_body(stats, msgId, email_body)


def main():
    os.makedirs(CBS_FILES_ROOT_PATH, exist_ok=True)
    with get_imap_session() as imap_session:
        msgIds = imap_search_all(imap_session)
        stats = defaultdict(int)
        _, df_stats = DF.Flow(
            process_msgIds(stats, imap_session, msgIds),
            DF.dump_to_path(CBS_EMAILS_DATA_ROOT_PATH)
        ).process()
        pprint(df_stats)
        pprint(dict(stats))
