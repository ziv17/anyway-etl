import subprocess

from .config import ANYWAY_KUBECTL_NAMESPACE


def check_call(*args):
    subprocess.check_call(['kubectl', '-n', ANYWAY_KUBECTL_NAMESPACE, 'exec', 'deployment/anyway-main', '--', *args])
