# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in *offline* mode using only configuration values.

    The context is configured from ``alembic.ini`` without creating an Engine.
    This allows Alembic to emit SQL without a live DBAPI connection.

    Recognized options (from ``alembic.ini``):
      - ``sqlalchemy.url``: database URL (may be omitted when using ``dialect``).
      - ``dialect``: force a dialect name when no URL is supplied.
      - ``version_table_schema``: schema that owns Alembic's version table.
      - ``starting_rev``: optional starting revision to limit the run.

    The environment enables ``include_schemas=True`` so objects outside the
    default schema are included, and uses ``literal_binds=True`` to produce
    fully rendered SQL.
    """

    # try getting url & version_table_schema
    url = config.get_main_option("sqlalchemy.url")
    version_table_schema = config.get_main_option("version_table_schema")

    # try getting dialect in case URL has not been specified
    dialect = config.get_main_option("dialect")

    # try getting starting revision
    starting_rev = config.get_main_option("starting_rev")

    context.configure(
        url=url,
        version_table_schema=version_table_schema,
        dialect_name=dialect,
        starting_rev=starting_rev,
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True)

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in *online* mode against a live database connection.

    An Engine is constructed from the ``alembic.ini`` section. For databases
    that support named schemas (e.g., PostgreSQL), we set a
    ``schema_translate_map`` so that ``None`` maps to ``version_table_schema``
    (if provided). The Alembic context is configured with:
      - the active connection,
      - the resolved ``version_table_schema``, and
      - ``include_schemas=True`` so all schemas are migrated.

    Migrations are then executed inside a transaction.
    """

    params = config.get_section(config.config_ini_section)

    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool)

    with connectable.connect() as connection:
        # Forcing the default is needed for PostgreSQL installations with named schemas.
        # For other databases it doesn't matter.
        # https://github.com/sqlalchemy/alembic/issues/409
        conn = connection.execution_options(schema_translate_map={None: params.get('version_table_schema', None)})

        context.configure(
            connection=conn,
            target_metadata=target_metadata,
            version_table_schema=params.get('version_table_schema', None),
            include_schemas=True)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
