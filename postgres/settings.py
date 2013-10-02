from django_productline.context import PRODUCT_CONTEXT


if hasattr(PRODUCT_CONTEXT, 'DATABASES'):
    # set DATABASES from PRODUCT_CONTEXT
    refine_DATABASES = PRODUCT_CONTEXT.DATABASES
else:
    raise AttributeError('''
        postgres feature is selected but configuration is missing in PRODUCT_CONTEXT.
        Did you run "ape generate_context"?
    ''')
