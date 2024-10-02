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
