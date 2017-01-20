from __future__ import unicode_literals, print_function
from django.conf import settings
from subprocess import Popen, PIPE, STDOUT
import tempfile



class DumpDataError(Exception):
    """
    Raise whenever a dump error occurs
    """

def dump_database(host, db_name):
    """
    Dumps the passed database for the current product.
    :param db_name:
    :return:
    """

    temp = tempfile.NamedTemporaryFile()

    p = Popen(['pg_dump', '--no-owner', '--host', host, '--username', 'postgres', '-f', temp.name , db_name], stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    p.communicate()[0]
    dump = temp.read()
    temp.close()

    if len(dump) < 200:
        # in case the dump has less than 5 lines, it probably failed
        raise DumpDataError('Dumping database failed: {content}'.format(content=dump))

    return dump




def restore_database(target_path, db_name, owner):
    """
    Restore a postgresql database from a dumpfile.
    :param target_path:
    :param db_name:
    :param owner:
    :return:
    """

    from django.conf import settings
    host = settings.DATABASES['default']['HOST']

    path = '{path}'.format(path=target_path)

    p = Popen(['psql', '--host', host, '--username', owner, '-f', path, db_name], stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    r = p.communicate()[0]
    print(r)


