
def refine_DATABASES(orginal):
    import sys
    from django_productline.context import PRODUCT_CONTEXT
    # return db_host with postgres as user to be able to create test_dbs
    if 'run_product_tests' in sys.argv or 'test' in sys.argv:
        return {
            'default' : {
                'ENGINE': 'django.db.backends.postgresql',
                'HOST': PRODUCT_CONTEXT.DB_HOST,
                'USER': 'postgres',
            }
        }
    # set DATABASES from PRODUCT_CONTEXT
    return {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'HOST': PRODUCT_CONTEXT.DB_HOST,
            'NAME': PRODUCT_CONTEXT.DB_NAME,
            'USER': PRODUCT_CONTEXT.DB_USER,
            'PASSWORD': PRODUCT_CONTEXT.DB_PASSWORD,
        }
    }
