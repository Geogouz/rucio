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

''' Add PREPARING state to Request model. '''

from alembic import context, op

from rucio.db.sqla.migrate_repo import create_enum_sql, drop_enum_sql, enum_values_clause, render_enum_name
from rucio.db.sqla.migrate_repo import try_drop_constraint

# Alembic revision identifiers
revision = 'd23453595260'
down_revision = '8ea9122275b1'


def upgrade():
    """Upgrade the database to this revision"""

    new_enum_values = ['Q', 'G', 'S', 'D', 'F', 'L', 'N', 'O', 'A', 'U', 'W', 'M', 'P']

    schema = context.get_context().version_table_schema + '.' if context.get_context().version_table_schema else ''
    dialect = context.get_context().dialect.name

    if dialect == 'oracle':
        try_drop_constraint('REQUESTS_STATE_CHK', 'requests')
        op.create_check_constraint(
            constraint_name='REQUESTS_STATE_CHK',
            table_name='requests',
            condition=f'state in ({enum_values_clause(new_enum_values)})',
        )
    elif dialect == 'postgresql':
        requests_history_enum = render_enum_name('REQUESTS_HISTORY_STATE_CHK')
        requests_enum = render_enum_name('REQUESTS_STATE_CHK')

        op.execute('ALTER TABLE %srequests_history DROP CONSTRAINT IF EXISTS "REQUESTS_HISTORY_STATE_CHK", ALTER COLUMN state TYPE CHAR' % schema)
        op.execute(drop_enum_sql('REQUESTS_HISTORY_STATE_CHK'))
        op.execute(create_enum_sql('REQUESTS_HISTORY_STATE_CHK', new_enum_values))
        op.execute(f'ALTER TABLE {schema}requests_history ALTER COLUMN state TYPE {requests_history_enum} USING state::{requests_history_enum}')
        op.execute('ALTER TABLE %srequests DROP CONSTRAINT IF EXISTS "REQUESTS_STATE_CHK", ALTER COLUMN state TYPE CHAR' % schema)
        op.execute(drop_enum_sql('REQUESTS_STATE_CHK'))
        op.execute(create_enum_sql('REQUESTS_STATE_CHK', new_enum_values))
        op.execute(f'ALTER TABLE {schema}requests ALTER COLUMN state TYPE {requests_enum} USING state::{requests_enum}')

    elif dialect == 'mysql':
        if context.get_context().dialect.server_version_info[0] == 8:
            op.drop_constraint('REQUESTS_STATE_CHK', 'requests', type_='check')

        op.create_check_constraint(
            constraint_name='REQUESTS_STATE_CHK',
            table_name='requests',
            condition=f'state in ({enum_values_clause(new_enum_values)})',
        )


def downgrade():
    """Downgrade the database to the previous revision"""

    old_enum_values = ['Q', 'G', 'S', 'D', 'F', 'L', 'N', 'O', 'A', 'U', 'W', 'M']

    schema = context.get_context().version_table_schema + '.' if context.get_context().version_table_schema else ''
    dialect = context.get_context().dialect.name

    if dialect == 'oracle':
        try_drop_constraint('REQUESTS_STATE_CHK', 'requests')
        op.create_check_constraint(
            constraint_name='REQUESTS_STATE_CHK',
            table_name='requests',
            condition=f'state in ({enum_values_clause(old_enum_values)})',
        )
    elif dialect == 'postgresql':
        requests_history_enum = render_enum_name('REQUESTS_HISTORY_STATE_CHK')
        requests_enum = render_enum_name('REQUESTS_STATE_CHK')

        op.execute('ALTER TABLE %srequests_history DROP CONSTRAINT IF EXISTS "REQUESTS_HISTORY_STATE_CHK", ALTER COLUMN state TYPE CHAR' % schema)
        op.execute(drop_enum_sql('REQUESTS_HISTORY_STATE_CHK'))
        op.execute(create_enum_sql('REQUESTS_HISTORY_STATE_CHK', old_enum_values))
        op.execute(f'ALTER TABLE {schema}requests_history ALTER COLUMN state TYPE {requests_history_enum} USING state::{requests_history_enum}')
        op.execute('ALTER TABLE %srequests DROP CONSTRAINT IF EXISTS "REQUESTS_STATE_CHK", ALTER COLUMN state TYPE CHAR' % schema)
        op.execute(drop_enum_sql('REQUESTS_STATE_CHK'))
        op.execute(create_enum_sql('REQUESTS_STATE_CHK', old_enum_values))
        op.execute(f'ALTER TABLE {schema}requests ALTER COLUMN state TYPE {requests_enum} USING state::{requests_enum}')

    elif dialect == 'mysql':
        op.create_check_constraint(
            constraint_name='REQUESTS_STATE_CHK',
            table_name='requests',
            condition=f'state in ({enum_values_clause(old_enum_values)})',
        )

        if context.get_context().dialect.server_version_info[0] == 8:
            op.drop_constraint('REQUESTS_STATE_CHK', 'requests', type_='check')
