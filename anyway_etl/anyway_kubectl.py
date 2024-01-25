import subprocess

from .config import ANYWAY_MAIN_CONTAINER_NAME


def check_call(*args):
    subprocess.check_call([
        'docker', 'exec', ANYWAY_MAIN_CONTAINER_NAME, *args
    ])
