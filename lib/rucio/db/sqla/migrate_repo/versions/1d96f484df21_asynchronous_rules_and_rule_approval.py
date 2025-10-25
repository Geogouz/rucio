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

''' asynchronous rules and rule approval '''

import sqlalchemy as sa
from alembic import op
from alembic.op import add_column, create_check_constraint, drop_column

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
revision = '1d96f484df21'
down_revision = '3d9813fab443'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    schema = get_effective_schema()
    rules_table = qualify_table('rules', schema)

    if is_current_dialect('oracle'):
        add_column('rules', sa.Column('ignore_account_limit', sa.Boolean(name='RULES_IGNORE_ACCOUNT_LIMIT_CHK', create_constraint=True), default=False))
        try_drop_constraint('RULES_STATE_CHK', 'rules')
        create_check_constraint('RULES_STATE_CHK', 'rules', "state IN ('S', 'R', 'U', 'O', 'W', 'I')")

    elif is_current_dialect('postgresql'):
        add_column('rules', sa.Column('ignore_account_limit', sa.Boolean(name='RULES_IGNORE_ACCOUNT_LIMIT_CHK', create_constraint=True), default=False), schema=schema)
        op.execute(
            f'ALTER TABLE {rules_table} DROP CONSTRAINT IF EXISTS "RULES_STATE_CHK"'
        )
        op.execute(
            alter_enum_add_value_sql(
                'RULES_STATE_CHK',
                'W',
                schema=schema,
                if_not_exists=True,
                after='O',
            )
        )
        op.execute(
            alter_enum_add_value_sql(
                'RULES_STATE_CHK',
                'I',
                schema=schema,
                if_not_exists=True,
                after='W',
            )
        )

    elif is_current_dialect('mysql'):
        add_column('rules', sa.Column('ignore_account_limit', sa.Boolean(name='RULES_IGNORE_ACCOUNT_LIMIT_CHK', create_constraint=True), default=False), schema=schema)
        op.execute(f'ALTER TABLE {rules_table} DROP CHECK RULES_STATE_CHK')
        create_check_constraint('RULES_STATE_CHK', 'rules', "state IN ('S', 'R', 'U', 'O', 'W', 'I')")


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    schema = get_effective_schema()
    rules_table = qualify_table('rules', schema)

    if is_current_dialect('oracle'):
        drop_column('rules', 'ignore_account_limit')
        try_drop_constraint('RULES_STATE_CHK', 'rules')
        create_check_constraint('RULES_STATE_CHK', 'rules', "state IN ('S', 'R', 'U', 'O')")

    elif is_current_dialect('postgresql'):
        drop_column('rules', 'ignore_account_limit', schema=schema)
        op.execute(
            f'ALTER TABLE {rules_table} '
            'DROP CONSTRAINT IF EXISTS "RULES_STATE_CHK", ALTER COLUMN state TYPE CHAR'
        )
        op.execute(drop_enum_sql('RULES_STATE_CHK', schema=schema))
        op.execute(
            create_enum_if_absent_block(
                'RULES_STATE_CHK',
                ['S', 'R', 'U', 'O'],
                schema=schema,
            )
        )
        op.execute(
            f"""
            ALTER TABLE {rules_table}
            ALTER COLUMN state TYPE "RULES_STATE_CHK"
            USING state::"RULES_STATE_CHK"
            """
        )

    elif is_current_dialect('mysql'):
        drop_column('rules', 'ignore_account_limit', schema=schema)
        create_check_constraint('RULES_STATE_CHK', 'rules', "state IN ('S', 'R', 'U', 'O')")
