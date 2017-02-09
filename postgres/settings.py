
def refine_DATABASES(orginal):
    from django_productline.context import PRODUCT_CONTEXT
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
