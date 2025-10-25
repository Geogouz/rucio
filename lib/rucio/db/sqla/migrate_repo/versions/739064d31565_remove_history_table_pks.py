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

''' remove history table pks '''

from alembic.op import create_primary_key, drop_constraint

from rucio.db.sqla.migrate_repo import drop_current_primary_key, try_drop_constraint
from rucio.db.sqla.migrate_repo.ddl_helpers import get_current_dialect

# Alembic revision identifiers
revision = '739064d31565'
down_revision = 'ccdbcd48206e'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    dialect = get_current_dialect()

    if dialect in ['oracle', 'mysql', 'postgresql']:
        # CONFIGS_HISTORY
        if dialect in ['oracle', 'postgresql']:
            drop_current_primary_key('configs_history')
            for pk_name in ('CONFIGS_HISTORY_PK', 'configs_history_pk', 'configs_history_pkey', 'PRIMARY'):
                try_drop_constraint(pk_name, 'configs_history')
        else:
            drop_constraint('CONFIGS_HISTORY_PK', 'configs_history', type_='primary')


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    dialect = get_current_dialect()

    if dialect in ['oracle', 'mysql', 'postgresql']:
        if dialect in ['oracle', 'postgresql']:
            drop_current_primary_key('configs_history')
            for pk_name in ('CONFIGS_HISTORY_PK', 'configs_history_pk', 'configs_history_pkey', 'PRIMARY'):
                try_drop_constraint(pk_name, 'configs_history')
        create_primary_key('CONFIGS_HISTORY_PK', 'configs_history', ['section', 'opt', 'updated_at'])
