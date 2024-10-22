Basic CLI tool to handle ScyllaDB migration

Features:
- Create migration files
- Execute queries

Supported Commands
1. Generate: Generates empty migration files.
2. Up: Applies Migrations.
3. Down: Reverts Migrations. Use with --all attribute to revert all migrations.

Env:
1. `SCYLLADB_MIGRATE_DIR_PATH` to set path to migrations. The path must be a valid directory. Defaults to PWD
2. `SCYLLADB_MIGRATE_DB_URL` to set url of ScyllaDB. Only required when applying/reverting migrations

Args:
1. `-p` to pass path to migrations. The path must be a valid directory. Defaults to PWD
2. `-u` to pass url of ScyllaDB. Only required when applying/reverting migrations

Note that if both env and args are passed, args will override env
