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

''' add notification column to rules '''

import sqlalchemy as sa
from alembic import op
from alembic.op import add_column, drop_column

from rucio.db.sqla.constants import RuleNotification
from rucio.db.sqla.migrate_repo import (
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
revision = '4207be2fd914'
down_revision = '14ec5aeb64cf'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    schema = get_effective_schema()
    rules_table = qualify_table('rules', schema)

    if is_current_dialect('oracle', 'mysql'):
        add_column('rules', sa.Column('notification', sa.Enum(RuleNotification,
                                                              name='RULES_NOTIFICATION_CHK',
                                                              create_constraint=True,
                                                              values_callable=lambda obj: [e.value for e in obj]),
                                      default=RuleNotification.NO), schema=schema)
    elif is_current_dialect('postgresql'):
        enum_values = [notification.value for notification in RuleNotification]
        op.execute(
            create_enum_if_absent_block(
                'RULES_NOTIFICATION_CHK',
                enum_values,
                schema=schema,
            )
        )
        op.execute(
            f'ALTER TABLE {rules_table} ADD COLUMN notification "RULES_NOTIFICATION_CHK"'
        )


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    schema = get_effective_schema()
    rules_table = qualify_table('rules', schema)

    if is_current_dialect('oracle'):
        try_drop_constraint('RULES_NOTIFICATION_CHK', 'rules')
        drop_column('rules', 'notification', schema=schema)

    elif is_current_dialect('postgresql'):
        op.execute(
            f'ALTER TABLE {rules_table} '
            'DROP CONSTRAINT IF EXISTS "RULES_NOTIFICATION_CHK", ALTER COLUMN notification TYPE CHAR'
        )
        op.execute(f'ALTER TABLE {rules_table} DROP COLUMN notification')
        op.execute(drop_enum_sql('RULES_NOTIFICATION_CHK', schema=schema))

    elif is_current_dialect('mysql'):
        drop_column('rules', 'notification', schema=schema)
