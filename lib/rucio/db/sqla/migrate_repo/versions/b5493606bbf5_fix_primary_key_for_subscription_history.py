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

''' Fix primary key for subscription_history '''

from alembic.op import create_primary_key, drop_constraint

from rucio.db.sqla.migrate_repo import drop_current_primary_key, try_drop_constraint
from rucio.db.sqla.migrate_repo.ddl_helpers import get_current_dialect

# Alembic revision identifiers
revision = 'b5493606bbf5'
down_revision = 'a08fa8de1545'


def upgrade():
    '''
    Upgrade the database to this revision
    '''
    dialect = get_current_dialect()

    if dialect in ['oracle', 'mysql', 'postgresql']:
        if dialect in ['oracle', 'postgresql']:
            drop_current_primary_key('subscriptions_history')
            for pk_name in ('SUBSCRIPTIONS_PK', 'SUBSCRIPTIONS_HISTORY_PK', 'subscriptions_history_pk', 'subscriptions_history_pkey', 'PRIMARY'):
                try_drop_constraint(pk_name, 'subscriptions_history')
        else:
            drop_constraint(constraint_name='SUBSCRIPTIONS_PK', table_name='subscriptions_history', type_='primary')
        create_primary_key('SUBSCRIPTIONS_HISTORY_PK', 'subscriptions_history', ['id', 'updated_at'])


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''
    dialect = get_current_dialect()

    if dialect in ['oracle', 'mysql', 'postgresql']:
        if dialect in ['oracle', 'postgresql']:
            drop_current_primary_key('subscriptions_history')
            for pk_name in ('SUBSCRIPTIONS_HISTORY_PK', 'SUBSCRIPTIONS_PK', 'subscriptions_history_pk', 'subscriptions_history_pkey', 'PRIMARY'):
                try_drop_constraint(pk_name, 'subscriptions_history')
        else:
            drop_constraint(constraint_name='SUBSCRIPTIONS_HISTORY_PK', table_name='subscriptions_history', type_='primary')
        create_primary_key('SUBSCRIPTIONS_PK', 'subscriptions_history', ['id', 'updated_at'])
