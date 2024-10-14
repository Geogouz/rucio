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

''' Add structured_meta column to did_meta table '''

import sqlalchemy as sa
from alembic import context
from alembic.op import add_column, drop_column

from rucio.db.sqla.types import JSON

# Alembic revision identifiers
revision = 'f4a848d02e48'
down_revision = 'b0070f3695c8'


def upgrade():
    '''
    Upgrade the database to this revision
    '''

    if context.get_context().dialect.name in ['oracle', 'postgresql', 'mysql']:
        print(context.get_context().version_table_schema)
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        add_column('did_meta', sa.Column('structured_meta', JSON()), schema=schema)


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''

    if context.get_context().dialect.name in ['oracle', 'postgresql', 'mysql']:
        schema = context.get_context().version_table_schema if context.get_context().version_table_schema else ''
        drop_column('did_meta', 'structured_meta', schema=schema)
