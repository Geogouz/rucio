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

''' change tokens pk '''

from alembic.op import create_foreign_key, create_primary_key, drop_constraint

from rucio.db.sqla.migrate_repo import drop_current_primary_key, try_drop_constraint
from rucio.db.sqla.migrate_repo.ddl_helpers import is_current_dialect

# Alembic revision identifiers
revision = '2eef46be23d4'
down_revision = '58c8b78301ab'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    if is_current_dialect('oracle', 'mysql', 'postgresql'):
        drop_constraint('TOKENS_ACCOUNT_FK', 'tokens', type_='foreignkey')
        if is_current_dialect('oracle', 'postgresql'):
            drop_current_primary_key('tokens')
            for pk_name in ('TOKENS_PK', 'TOKENS_PRIMARY', 'TOKENS_PK_OLD', 'tokens_pk', 'tokens_pkey', 'PRIMARY'):
                try_drop_constraint(pk_name, 'tokens')
        else:
            drop_constraint('TOKENS_PK', 'tokens', type_='primary')
        create_primary_key('TOKENS_PK', 'tokens', ['token'])
        create_foreign_key('TOKENS_ACCOUNT_FK', 'tokens', 'accounts', ['account'], ['account'])


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    if is_current_dialect('oracle', 'mysql', 'postgresql'):
        drop_constraint('TOKENS_ACCOUNT_FK', 'tokens', type_='foreignkey')
        if is_current_dialect('oracle', 'postgresql'):
            drop_current_primary_key('tokens')
            for pk_name in ('TOKENS_PK', 'TOKENS_PRIMARY', 'TOKENS_PK_OLD', 'tokens_pk', 'tokens_pkey', 'PRIMARY'):
                try_drop_constraint(pk_name, 'tokens')
        else:
            drop_constraint('TOKENS_PK', 'tokens', type_='primary')
        create_primary_key('TOKENS_PK', 'tokens', ['account', 'token'])
        create_foreign_key('TOKENS_ACCOUNT_FK', 'tokens', 'accounts', ['account'], ['account'])
