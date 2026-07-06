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

"""Add OIDC_ALL identity type"""

from alembic import context, op
from alembic.op import create_check_constraint

from rucio.db.sqla.util import try_drop_constraint

# Alembic revision identifiers
revision = '6fecebaaa173'
down_revision = '3b943000da18'

CURRENT_IDENTITY_TYPES = "'X509', 'GSS', 'USERPASS', 'SSH', 'SAML', 'OIDC'"
NEW_IDENTITY_TYPES = CURRENT_IDENTITY_TYPES + ", 'OIDC_ALL'"


def _drop_identity_type_constraint(constraint_name, table_name):
    if context.get_context().dialect.name == 'postgresql':
        schema = context.get_context().version_table_schema + '.' if context.get_context().version_table_schema else ''
        op.execute(f'ALTER TABLE {schema}{table_name} DROP CONSTRAINT IF EXISTS "{constraint_name}"')
    else:
        try_drop_constraint(constraint_name, table_name)


def _replace_identity_type_constraints(identity_types):
    _drop_identity_type_constraint('IDENTITIES_TYPE_CHK', 'identities')
    create_check_constraint(constraint_name='IDENTITIES_TYPE_CHK',
                            table_name='identities',
                            condition=f"identity_type in ({identity_types})")

    _drop_identity_type_constraint('ACCOUNT_MAP_ID_TYPE_CHK', 'account_map')
    create_check_constraint(constraint_name='ACCOUNT_MAP_ID_TYPE_CHK',
                            table_name='account_map',
                            condition=f"identity_type in ({identity_types})")


def upgrade():
    """Upgrade the database to this revision."""
    if context.get_context().dialect.name in ['oracle', 'postgresql']:
        _replace_identity_type_constraints(NEW_IDENTITY_TYPES)


def downgrade():
    """Downgrade the database to the previous revision."""
    if context.get_context().dialect.name in ['oracle', 'postgresql']:
        _replace_identity_type_constraints(CURRENT_IDENTITY_TYPES)
