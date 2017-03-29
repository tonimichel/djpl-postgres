
def refine_DATABASES(orginal):
    import sys
    from django_productline.context import PRODUCT_CONTEXT
    # return orginal(sqlite) if this is a test run
    if 'run_product_tests' in sys.argv:
        return orginal
    # set DATABASES from PRODUCT_CONTEXT
    return {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'HOST': PRODUCT_CONTEXT.DB_HOST,
            'NAME': PRODUCT_CONTEXT.DB_NAME,
            'USER': PRODUCT_CONTEXT.DB_USER,
            'PASSWORD': PRODUCT_CONTEXT.DB_PASSWORD,
        }
    }
