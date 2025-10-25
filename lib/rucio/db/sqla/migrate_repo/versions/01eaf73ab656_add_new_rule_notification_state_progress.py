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

''' add new rule notification state progress '''

from alembic import op
from alembic.op import create_check_constraint

from rucio.db.sqla.migrate_repo import try_drop_constraint
from rucio.db.sqla.migrate_repo.ddl_helpers import (
    get_effective_schema,
    is_current_dialect,
    qualify_table,
)

# Alembic revision identifiers
revision = '01eaf73ab656'
down_revision = '90f47792bb76'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    schema = get_effective_schema()
    rules_table = qualify_table('rules', schema)

    if is_current_dialect('oracle'):
        try_drop_constraint('RULES_NOTIFICATION_CHK', 'rules')
        create_check_constraint(constraint_name='RULES_NOTIFICATION_CHK', table_name='rules',
                                condition="notification in ('Y', 'N', 'C', 'P')")

    elif is_current_dialect('postgresql'):
        op.execute(
            f'ALTER TABLE {rules_table} '
            'DROP CONSTRAINT IF EXISTS "RULES_NOTIFICATION_CHK", ALTER COLUMN notification TYPE CHAR'
        )
        op.execute('DROP TYPE "RULES_NOTIFICATION_CHK"')
        op.execute(
            """CREATE TYPE "RULES_NOTIFICATION_CHK" AS ENUM('Y', 'N', 'C', 'P')"""
        )
        op.execute(
            f"""
            ALTER TABLE {rules_table}
            ALTER COLUMN notification TYPE "RULES_NOTIFICATION_CHK"
            USING notification::"RULES_NOTIFICATION_CHK"
            """
        )

    elif is_current_dialect('mysql'):
        create_check_constraint(constraint_name='RULES_NOTIFICATION_CHK', table_name='rules',
                                condition="notification in ('Y', 'N', 'C', 'P')")


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    schema = get_effective_schema()
    rules_table = qualify_table('rules', schema)

    if is_current_dialect('oracle'):
        try_drop_constraint('RULES_NOTIFICATION_CHK', 'rules')
        create_check_constraint(constraint_name='RULES_NOTIFICATION_CHK', table_name='rules',
                                condition="notification in ('Y', 'N', 'C')")

    elif is_current_dialect('postgresql'):
        op.execute(
            f'ALTER TABLE {rules_table} '
            'DROP CONSTRAINT IF EXISTS "RULES_NOTIFICATION_CHK", ALTER COLUMN notification TYPE CHAR'
        )
        op.execute('DROP TYPE "RULES_NOTIFICATION_CHK"')
        op.execute(
            """CREATE TYPE "RULES_NOTIFICATION_CHK" AS ENUM('Y', 'N', 'C')"""
        )
        op.execute(
            f"""
            ALTER TABLE {rules_table}
            ALTER COLUMN notification TYPE "RULES_NOTIFICATION_CHK"
            USING notification::"RULES_NOTIFICATION_CHK"
            """
        )

    elif is_current_dialect('mysql'):
        create_check_constraint(constraint_name='RULES_NOTIFICATION_CHK', table_name='rules',
                                condition="notification in ('Y', 'N', 'C')")
