# Initial params could be foun in https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
#

# Source database config
source_config = {
    'user': 'user',
    'password': 'password',
    'host': '127.0.0.1',
    'port': 3307,
    'compress': True,
    'buffered': True
}

# Destination database config
destination_config = {
    'user': 'user',
    'password': 'password',
    'host': '127.0.0.1',
    'port': 3308,
    'compress': True,
    'buffered': True
}

# These are mysql system databases.
# They don't have to be migrated.
sys_databases = [
    'information_schema',
    'performance_schema',
    'sys',
    'mysql'
]

# This is the list of databases to migrate.
# If empty, all found databases (except sys_databases) will be migrated.
databases_to_migrate = []


# This is a list of databases won't be migrated.
# If empty, all databases in 'databases_to_migrate' list (all if it's empty) will be migrated.
databases_to_avoid = []
