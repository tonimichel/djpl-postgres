from ape import tasks
from random import choice
import os
import os.path


def get_pgpass_file():
    return '%s/.pgpass' % os.path.expanduser('~')
    

@tasks.register
@tasks.requires_product_environment
def pg_create_user(db_username, db_password=None):
    '''create a postgresql user'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
   
    # check that a .pgpass file exists
    pgpass_file = get_pgpass_file()
    if not os.path.isfile(pgpass_file):
        print '*** your .pgpass file does not exist yet. Create %s and execute this task again.' % pgpass_file
        return
    
    if not db_password:
        db_password = ''.join([choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for i in range(16)])

    os.system('psql --host %s --username %s -c "CREATE USER %s WITH PASSWORD \'%s\';"' % (
        db_host,
        'postgres',
        db_username,
        db_password
    ))
   
    # add user and password to .pgpass
    with open(pgpass_file, 'a') as f:
        f.write('%s:5432:*:%s:%s\n' % (db_host, db_username, db_password))
    
    print '*** User "%s" created with password "%s". All stored in "%s"' % (db_username, db_password, pgpass_file)


@tasks.register
@tasks.requires_product_environment
def pg_drop_user(db_username):
    '''remove a postgresql user'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    
    if db_username == 'postgres':
        print '*** Sorry, you cant drop user "postgres".'
        return
    
    r = os.system('psql --host %s --username %s -c "DROP ROLE %s;"' % (
        db_host,
        'postgres',
        db_username
    ))
    
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
        print '*** Removed user %s.' % db_username
    

@tasks.register
@tasks.requires_product_environment
def pg_create_db(db_name, owner):
    '''Create a postgresql database'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s -c "CREATE DATABASE %s WITH OWNER %s TEMPLATE template0 ENCODING \'UTF8\';"' % (
        db_host,
        'postgres',
        db_name,
        owner,
    ))
    

@tasks.register
@tasks.requires_product_environment
def pg_drop_db(db_name):
    '''drop a postgresql database'''
    
    if db_name in ('postgres', 'template1', 'template0'):
        print '*** You are not allowed to drop "%s"!' % db_name
        return
    
    print '** Backup database before dropping'
    tasks.pg_backup(db_name)
    
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s -c "DROP DATABASE %s;"' % (
        db_host,
        'postgres',
        db_name
    ))


@tasks.register
@tasks.requires_product_environment
def pg_rename_user(user, username):
    '''list all databases'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s -c ";ALTER USER %s RENAME TO %s;"' % (
        db_host,
        'postgres',
        user,
        username
    ))

@tasks.register
@tasks.requires_product_environment
def pg_list_dbs():
    '''list all databases'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s --list' % (
        db_host,
        'postgres'
    ))




@tasks.register
@tasks.requires_product_environment
def pg_list_users():
    '''list all users'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s -c "\\du;"' % (
        db_host,
        'postgres'
    ))



   
@tasks.register
@tasks.requires_product_environment
def pg_backup(database_name, suffix=None):
    '''backup a postgresql database'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    
    import datetime
    suffix = suffix or datetime.datetime.now().isoformat().replace(':','-').replace('.', '-')
    backup_name = database_name + '_' + suffix
    backup_dir = '%s/_backup/%s' % (PRODUCT_CONTEXT.APE_ROOT_DIR, backup_name)
    os.system('mkdir -p %s' % backup_dir)
    target_sql = backup_dir + '/dump.sql'
    os.system('pg_dump --no-owner --host %s --username %s -f %s %s' % (
        db_host,
        'postgres',
        target_sql,
        database_name
    ))
    print '*** database dumped to: ' + backup_dir
    os.system('tar -cvf %s/media.tar.gz -C %s .' % (backup_dir, PRODUCT_CONTEXT.DATA_DIR))
    print '*** __data__ compressed to: ' + backup_dir
    return backup_name
    

@tasks.register
@tasks.requires_product_environment
def pg_rename_db(db_name, new_name):
    '''rename a postgresql database'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s -c "ALTER DATABASE %s RENAME TO %s;"' % (
        db_host,
        'postgres',
        db_name,
        new_name,
    ))
    print '*** Renamed db from "%s" to "%s"' % (db_name, new_name)


@tasks.register
@tasks.requires_product_environment
def pg_restore(backup_name, db_name, owner):
    '''restore a postgresql database from a dumpfile'''
    from django_productline.context import PRODUCT_CONTEXT
    db_host = PRODUCT_CONTEXT.DATABASES['default']['HOST']
    os.system('psql --host %s --username %s -f \'%s\' %s;' % (
        db_host,
        owner,
        '%s/_backup/%s' % (PRODUCT_CONTEXT.APE_ROOT_DIR, backup_name),
        db_name,
    ))
