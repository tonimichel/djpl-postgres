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
            db_name=settings.PRODUCT_CONTEXT.DB_NAME
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
    def refinement(target_path, db_name, db_owner):
        """
        Import from sql-file or dump.sql in zip-archive
        :param target_path:
        :return:
        """
        import zipfile
        import tempfile
        from . import api
        dump = target_path
        delete_dump = False

        # extract dump if zip file given
        if zipfile.is_zipfile(target_path):

            with zipfile.ZipFile(target_path) as zf:
                temp = tempfile.NamedTemporaryFile(delete=False)

                dump_fn = 'dump.sql'

                temp.write(zf.read(dump_fn))
                temp.flush()
                temp.close()
                dump = temp.name
                delete_dump = True

        original(dump, db_name, db_owner)
        api.restore_database(dump, db_name, db_owner)

        if delete_dump:
            os.unlink(dump)


    return refinement


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
    jsondata['DB_NAME'] = pg_name
    jsondata['DB_PASSWORD'] = pg_password
    jsondata['DB_USER'] = pg_user
    jsondata['DB_HOST'] = pg_host
    tasks.inject_context(json.dumps(jsondata))

def get_pgpass_file():
    return '{ext_path}/.pgpass'.format(ext_path=os.path.expanduser('~'))


def refine_get_context_template(original):
    """
     Refines ``ape.helpers.get_context_template`` and append postgres-specific context keys.
    :param original:
    :return:
    """

    def get_context():
        context = original()
        context.update({
            'DB_HOST': '',
            'DB_PASSWORD': '',
            'DB_NAME': '',
            'DB_USER': ''
        })
        return context
    return get_context

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

    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '-c', 'CREATE USER {db_username} WITH PASSWORD \'{db_password}\';'.format(
                db_username=db_username, db_password=db_password
            )
        ]
    )

    # add user and password to .pgpass
    with open(pgpass_file, 'a') as f:
        f.write('{db_host}:5432:*:{db_username}:{db_password}\n'.format(
            db_host=db_host,
            db_username=db_username,
            db_password=db_password
        ))

    print('*** User "{db_username}" created with password "{db_password}". All stored in "{pgpass_file}"'.format(
        db_username=db_username,
        db_password=db_password,
        pgpass_file=pgpass_file
    ))
    return db_password


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

    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '-c', 'DROP ROLE {db_username};'.format(db_username=db_username)
        ]
    )

    # update .pgpass file
    pgpass_file = get_pgpass_file()
    f = open(pgpass_file, 'r')
    lines = f.readlines()
    f.close()
    f = open(pgpass_file, 'w')
    for line in lines:
        if not line.startswith('{db_host}:5432:*:{db_username}:'.format(db_host=db_host, db_username=db_username)):
            f.write(line)
    f.close()
    # Raises, so no need to check subprocess error code
    print('*** Removed user {db_username}.'.format(db_username=db_username))


@tasks.register
@tasks.requires_product_environment
def pg_create_db(db_name, owner):
    """
    Create a postgresql database
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '-c', 'CREATE DATABASE {db_name} WITH OWNER {owner} TEMPLATE template0 ENCODING \'UTF8\';'.format(
                db_name=db_name,
                owner=owner
            )
        ]
    )


@tasks.register
@tasks.requires_product_environment
def pg_drop_db(db_name, backup_before=True):
    """
    Drop a postgresql database
    """

    if db_name in ('postgres', 'template1', 'template0'):
        print('*** You are not allowed to drop "{db_name}"!'.format(db_name=db_name))
        return

    if backup_before:
        print('** Backup database before dropping')
        tasks.pg_backup(db_name)

    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '-c', 'DROP DATABASE {db_name};'.format(db_name=db_name)
        ]
    )


@tasks.register
@tasks.requires_product_environment
def pg_list_dbs():
    """
    List all databases
    """
    from django.conf import settings

    print('... listing all databases. Type "q" to quit.')

    db_host = settings.DATABASES['default']['HOST']
    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '--list'
        ]
    )


@tasks.register
@tasks.requires_product_environment
def pg_list_users():
    """
    List all users
    """
    from django.conf import settings
    db_host = settings.DATABASES['default']['HOST']
    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '-c', '\\du'
        ]
    )



@tasks.register
@tasks.requires_product_environment
def pg_backup(database_name, suffix=None):
    """
    Backup a postgresql database
    """
    # TODO: deprecated
    from django.conf import settings
    from django_productline.context import PRODUCT_CONTEXT
    db_host = settings.DATABASES['default']['HOST']

    import datetime
    suffix = suffix or datetime.datetime.now().isoformat().replace(':','-').replace('.', '-')
    backup_name = database_name + '_' + suffix
    backup_dir = '{ape_root_dir}/_backup/{backup_name}'.format(
        ape_root_dir=PRODUCT_CONTEXT.APE_ROOT_DIR,
        backup_name=backup_name
    )
    subprocess.check_call(
        [
            'mkdir',
            '-p', backup_dir
        ]
    )
    target_sql = backup_dir + '/dump.sql'
    subprocess.check_call(
        [
            'pg_dump',
            '--no-owner',
            '--host', db_host,
            '--username', 'postgres',
            '-f', target_sql, database_name
        ]
    )
    print('*** database dumped to: ' + backup_dir)
    subprocess.check_call(
        [
            'tar',
            '-cvf', '{backup_dir}/media.tar.gz'.format(backup_dir=backup_dir),
            '-C', PRODUCT_CONTEXT.DATA_DIR, '.'
        ]
    )
    tasks.export_context(os.path.join(backup_dir, 'context.zip'))
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
    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', 'postgres',
            '-c', 'ALTER DATABASE {db_name} RENAME TO {new_name};'.format(db_name=db_name, new_name=new_name)
        ]
    )
    print('*** Renamed db from "{db_name}" to "{new_name}"'.format(db_name=db_name, new_name=new_name))


@tasks.register
@tasks.requires_product_environment
def pg_restore(backup_name, db_name, owner):
    """
    Restore a postgresql database from a dumpfile
    """
    from django.conf import settings
    from django_productline.context import PRODUCT_CONTEXT
    db_host = settings.DATABASES['default']['HOST']
    subprocess.check_call(
        [
            'psql',
            '--host', db_host,
            '--username', owner,
            '-f', '{ape_root_dir}/_backup/{backup_name}'.format(
                ape_root_dir=PRODUCT_CONTEXT.APE_ROOT_DIR,
                backup_name=backup_name
            ), db_name
        ]
    )


@tasks.register
@tasks.requires_product_environment
def pg_reset_database(backup_name, db_name, owner):
    """
    Drop database, create database and restore from backup.
    """

    tasks.pg_drop_db(db_name, False)
    tasks.pg_create_db(db_name, owner)
    tasks.pg_restore(backup_name, db_name, owner)
    print("*** Resetted database {db_name} with backup {backup_name}".format(db_name=db_name, backup_name=backup_name))
