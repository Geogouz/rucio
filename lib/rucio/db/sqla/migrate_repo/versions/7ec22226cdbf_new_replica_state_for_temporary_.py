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

''' new replica state for temporary unavailable replicas '''

from alembic import op
from alembic.op import create_check_constraint

from rucio.db.sqla.migrate_repo import (
    alter_enum_add_value_sql,
    create_enum_if_absent_block,
    drop_enum_sql,
    try_drop_constraint,
)
from rucio.db.sqla.migrate_repo.ddl_helpers import (
    get_effective_schema,
    is_current_dialect,
    qualify_table,
)

# Alembic revision identifiers
revision = '7ec22226cdbf'
down_revision = '3345511706b8'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    schema = get_effective_schema()
    replicas_table = qualify_table('replicas', schema)

    if is_current_dialect('oracle'):
        try_drop_constraint('REPLICAS_STATE_CHK', 'replicas')
        create_check_constraint(constraint_name='REPLICAS_STATE_CHK', table_name='replicas',
                                condition="state in ('A', 'U', 'C', 'B', 'D', 'S', 'T')")

    elif is_current_dialect('postgresql'):
        op.execute(
            f'ALTER TABLE {replicas_table} DROP CONSTRAINT IF EXISTS "REPLICAS_STATE_CHK"'
        )
        op.execute(
            alter_enum_add_value_sql(
                'REPLICAS_STATE_CHK',
                'T',
                schema=schema,
                if_not_exists=True,
                after='S',
            )
        )

    elif is_current_dialect('mysql'):
        op.execute(f'ALTER TABLE {replicas_table} DROP CHECK REPLICAS_STATE_CHK')
        create_check_constraint(constraint_name='REPLICAS_STATE_CHK', table_name='replicas',
                                condition="state in ('A', 'U', 'C', 'B', 'D', 'S', 'T')")


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    schema = get_effective_schema()
    replicas_table = qualify_table('replicas', schema)

    if is_current_dialect('oracle'):
        try_drop_constraint('REPLICAS_STATE_CHK', 'replicas')
        create_check_constraint(constraint_name='REPLICAS_STATE_CHK', table_name='replicas',
                                condition="state in ('A', 'U', 'C', 'B', 'D', 'S')")

    elif is_current_dialect('postgresql'):
        op.execute(
            f'ALTER TABLE {replicas_table} '
            'DROP CONSTRAINT IF EXISTS "REPLICAS_STATE_CHK", ALTER COLUMN state TYPE CHAR'
        )
        op.execute(drop_enum_sql('REPLICAS_STATE_CHK', schema=schema))
        op.execute(
            create_enum_if_absent_block(
                'REPLICAS_STATE_CHK',
                ['A', 'U', 'C', 'B', 'D', 'S'],
                schema=schema,
            )
        )
        op.execute(
            f"""
            ALTER TABLE {replicas_table}
            ALTER COLUMN state TYPE "REPLICAS_STATE_CHK"
            USING state::"REPLICAS_STATE_CHK"
            """
        )

    elif is_current_dialect('mysql'):
        op.execute(f'ALTER TABLE {replicas_table} DROP CHECK REPLICAS_STATE_CHK')
        create_check_constraint(constraint_name='REPLICAS_STATE_CHK', table_name='replicas',
                                condition="state in ('A', 'U', 'C', 'B', 'D', 'S')")
