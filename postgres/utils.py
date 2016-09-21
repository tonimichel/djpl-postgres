from __future__ import unicode_literals, print_function
from django.conf import settings
from subprocess import Popen, PIPE, STDOUT



class DumpDataError(Exception):
    """
    Raise whenever a dump error occurs
    """

def dump_database(database_name):
    """
    Dumps the passed database for the current product.
    :param database_name:
    :return:
    """

    host = settings.DATABASES['default']['HOST']
    p = Popen(['pg_dump', '--no-owner', '--host', host, '--username', 'postgres', database_name], stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    dump = p.communicate()[0]

    if len(dump) < 200:
        # in case the dump has less than 5 lines, it probably failed
        raise DumpDataError('Dumping database failed: {content}'.format(content=dump))

    return dump

