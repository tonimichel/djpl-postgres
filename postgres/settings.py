def refine_DATABASES(original):
    return {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2', 
            'HOST': '{{ pg_host }}', 
            'PASSWORD': '{{ pg_password }}', 
            'NAME': '{{ pg_name }}', 
            'USER': '{{ pg_username }}'
        }
    }

