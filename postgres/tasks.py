from __future__ import unicode_literals, print_function
from ape import tasks
from random import choice
import os
import os.path
import subprocess
import glob
import json


def refine_export_database(original):

    @tasks.requires_product_environment
    def export_database(target_path):
        """
        Exports the database. In case target_path is an zip-archive, it is added to this archive.
        Otherwise it is written to a file.
        :param target_path:
        :return:
        """
        import tempfile
        import codecs
        from . import api
        from django.conf import settings
        from django_productline import utils
        # call original
        original(target_path)

        # create the dump
        dump = api.dump_database(
            host=settings.DATABASES['default']['HOST'],
            db_name=settings.PRODUCT_CONTEXT.PG_NAME
        )

        if target_path.endswith('.zip'):
            # add the dump to the archive in case the target path is a zip
            temp = tempfile.NamedTemporaryFile()
            temp.write(dump)
            temp.flush()
            utils.create_or_append_to_zip(temp.name, target_path, 'dump.sql')
            temp.close()
        else:
            # write the dump to an ordinary files
            # TODO: why do we get encoding errors when endcoding='utf-8' <- error on mac, cannot reproduce on linux
            with codecs.open(target_path, 'w') as f:
                f.write(dump)

        return target_path

    return export_database



def refine_import_database(original):

    @tasks.requires_product_environment
    def new_impl(target_path, db_name, db_owner):
        """
        :param target_path:
        :return:
        """
        import zipfile
        import tempfile
        from . import api
        dump = target_path

        # extract dump if zip file given
        if target_path.endswith('.zip'):

            with zipfile.ZipFile(target_path) as unzipped_data:
                temp = tempfile.NamedTemporaryFile()
                temp.write(unzipped_data.read('dump.sql'))
                temp.flush()

            dump = temp.name

        original(dump, db_name, db_owner)
        api.restore_database(dump, db_name, db_owner)

        if temp:
            temp.close()

    return new_impl


@tasks.register
def config_db(pg_name, pg_password, pg_user, pg_host):
    """
    Configure postgres settings, facade to inject_context
    :param pg_name:
    :param pg_password:
    :param pg_user:
    :param pg_host:
    :return:
    """
    jsondata = dict()
    jsondata['PG_NAME'] = pg_name
    jsondata['PG_PASSWORD'] = pg_password
    jsondata['PG_USER'] = pg_user
    jsondata['PG_HOST'] = pg_host
    tasks.inject_context(json.dumps(jsondata))

def get_pgpass_file():
    return '%s/.pgpass' % os.path.expanduser('~')


def refine_get_context_template(original):
    """
     Refines ``ape.helpers.get_context_template`` and append postgres-specific context keys.
    :param original:
    :return:
    """

    def get_context():
        context = original()
        context.update({
            'PG_HOST': '',
            'PG_PASSWORD': '',
            'PG_NAME': '',
            'PG_USER': ''
        })
        return context
    return get_context



def refine_install_dependencies(original):
    """
    Add install_pycopg2 to post_install_container.
    :param original:
    :return:
    """

    def install():
        original()
        tasks.pg_install_psycopg2()
    return install





@tasks.register
@tasks.requires_product_environment
def pg_create_user(db_username, db_password=None):
    """
    Create a postgresql user
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']

    # check that a .pgpass file exists
    pgpass_file = get_pgpass_file()
    if not os.path.isfile(pgpass_file):
        print('*** your .pgpass file does not exist yet. Create {passfile} and execute this task again.'.format(passfile=pgpass_file))
        return

    if not db_password:
        db_password = ''.join([choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for i in range(16)])

    print(
        subprocess.check_output('psql --host %s --username %s -c "CREATE USER %s WITH PASSWORD \'%s\';"' % (
            db_host,
            'postgres',
            db_username,
            db_password
        ), shell=True)
    )

    # add user and password to .pgpass
    with open(pgpass_file, 'a') as f:
        f.write('%s:5432:*:%s:%s\n' % (db_host, db_username, db_password))

    print('*** User "%s" created with password "%s". All stored in "%s"' % (db_username, db_password, pgpass_file))


@tasks.register
@tasks.requires_product_environment
def pg_drop_user(db_username):
    """
    Remove a postgresql user
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']

    if db_username == 'postgres':
        print('*** Sorry, you cant drop user "postgres".')
        return

    r = subprocess.check_output('psql --host %s --username %s -c "DROP ROLE %s;"' % (
        db_host,
        'postgres',
        db_username
    ), shell=True)
    print(r)

    # update .pgpass file
    pgpass_file = get_pgpass_file()
    f = open(pgpass_file, 'r')
    lines = f.readlines()
    f.close()
    f = open(pgpass_file, 'w')
    for line in lines:
        if not line.startswith('%s:5432:*:%s:' % (db_host, db_username)):
            f.write(line)
    f.close()

    if r == 0:
        print('*** Removed user %s.' % db_username)


@tasks.register
@tasks.requires_product_environment
def pg_create_db(db_name, owner):
    """
    Create a postgresql database
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s -c "CREATE DATABASE %s WITH OWNER %s TEMPLATE template0 ENCODING \'UTF8\';"' % (
        db_host,
        'postgres',
        db_name,
        owner,
    ), shell=True))


@tasks.register
@tasks.requires_product_environment
def pg_drop_db(db_name, backup_before=True):
    """
    Drop a postgresql database
    """

    if db_name in ('postgres', 'template1', 'template0'):
        print('*** You are not allowed to drop "%s"!' % db_name)
        return

    if backup_before:
        print('** Backup database before dropping')
        tasks.pg_backup(db_name)

    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s -c "DROP DATABASE %s;"' % (
        db_host,
        'postgres',
        db_name
    ), shell=True))


@tasks.register
@tasks.requires_product_environment
def pg_rename_user(user, username):
    """
    List all databases
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s -c ";ALTER USER %s RENAME TO %s;"' % (
        db_host,
        'postgres',
        user,
        username
    ), shell=True))

@tasks.register
@tasks.requires_product_environment
def pg_list_dbs():
    """
    List all databases
    """
    from django.conf import settings

    print('... listing all databases. Type "q" to quit.')

    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s --list' % (
        db_host,
        'postgres'
    ), shell=True))


@tasks.register
@tasks.requires_product_environment
def pg_list_users():
    """
    List all users
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s -c "\\du;"' % (
        db_host,
        'postgres'
    ), shell=True))



@tasks.register
@tasks.requires_product_environment
def pg_backup(database_name, suffix=None):
    """
    Backup a postgresql database
    """
    from django.conf import settings
    from django_productline.context import PRODUCT_CONTEXT
    db_host = settings.DATABASES['default']['HOST']

    import datetime
    suffix = suffix or datetime.datetime.now().isoformat().replace(':','-').replace('.', '-')
    backup_name = database_name + '_' + suffix
    backup_dir = '%s/_backup/%s' % (PRODUCT_CONTEXT.APE_ROOT_DIR, backup_name)
    print(subprocess.check_output('mkdir -p %s' % backup_dir, shell=True))
    target_sql = backup_dir + '/dump.sql'
    print(subprocess.check_output('pg_dump --no-owner --host %s --username %s -f %s %s' % (
        db_host,
        'postgres',
        target_sql,
        database_name
    ), shell=True))
    print('*** database dumped to: ' + backup_dir)
    print(subprocess.check_output('tar -cvf %s/media.tar.gz -C %s .' % (backup_dir, PRODUCT_CONTEXT.DATA_DIR), shell=True))
    print('*** __data__ compressed to: ' + backup_dir)
    return backup_name


@tasks.register
@tasks.requires_product_environment
def pg_rename_db(db_name, new_name):
    """
    Rename a postgresql database
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s -c "ALTER DATABASE %s RENAME TO %s;"' % (
        db_host,
        'postgres',
        db_name,
        new_name,
    ), shell=True))
    print('*** Renamed db from "%s" to "%s"' % (db_name, new_name))


@tasks.register
@tasks.requires_product_environment
def pg_restore(backup_name, db_name, owner):
    """
    Restore a postgresql database from a dumpfile
    """
    from django.conf import settings
    from django_productline.context import PRODUCT_CONTEXT
    db_host = settings.DATABASES['default']['HOST']
    print(subprocess.check_output('psql --host %s --username %s -f \'%s\' %s;' % (
        db_host,
        owner,
        '%s/_backup/%s' % (PRODUCT_CONTEXT.APE_ROOT_DIR, backup_name),
        db_name,
    ), shell=True))

@tasks.register
@tasks.requires_product_environment
def pg_reset_database(backup_name, db_name, owner):
    """
    Drop database, create database and restore from backup.
    """

    tasks.pg_drop_db(db_name, False)
    tasks.pg_create_db(db_name, owner)
    tasks.pg_restore(backup_name, db_name, owner)
    print("*** Resetted database %s with backup %s" % (db_name, backup_name))


@tasks.register
def pg_install_psycopg2():
    """
    Install psycopg2 to container-level venv.
    """

    try:
        import psycopg2
        print('...skipping: psycopg2 is already installed')
        return
    except ImportError:
        print('... installing psycopg2')

    p = subprocess.Popen(
        '/usr/bin/python -c "import psycopg2; print psycopg2.__file__"',
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE
    )
    r = p.communicate()[0]
    if not r:
        print('ERROR: Please install psycopg first: sudo apt-get install libpq-dev python-dev python-psycopg2; Make sure ape is not activated;')
        return

    psycodir = os.path.dirname(r)
    mxdir = '/'.join(psycodir.split('/')[:-1]) + '/mx'
    sitepackages = glob.glob('%s/_lib/venv/lib/*/site-packages/' % os.environ['CONTAINER_DIR'])[0]

    print(subprocess.check_output(
        'ln -s %s %s' % (psycodir, sitepackages) +
        'ln -s %s %s' % (mxdir, sitepackages), shell=True
    ))
    print('*** Successfully installed psycopg2')
