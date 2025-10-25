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

''' rename callback to message '''

from alembic import op
from alembic.op import create_check_constraint, create_primary_key, drop_constraint, rename_table

from rucio.db.sqla.migrate_repo import drop_current_primary_key, try_drop_constraint
from rucio.db.sqla.migrate_repo.ddl_helpers import (
    get_effective_schema,
    is_current_dialect,
    qualify_table,
)

# Alembic revision identifiers
revision = '58c8b78301ab'
down_revision = '2b8e7bcb4783'


def upgrade():
    '''
    Upgrade the database to this revision
    '''
    schema = get_effective_schema()

    if is_current_dialect('oracle'):
        # Drop PK on source (callbacks) if present, then rename to messages.
        drop_current_primary_key('callbacks')
        for pk_name in ('CALLBACKS_PK', 'callbacks_pk', 'callbacks_pkey', 'PRIMARY'):
            try_drop_constraint(pk_name, 'callbacks')

        rename_table('callbacks', 'messages')

        # Idempotency: remove any leftover constraints with either legacy or target names.
        for chk in (
            'CALLBACKS_EVENT_TYPE_NN', 'CALLBACKS_PAYLOAD_NN', 'CALLBACKS_CREATED_NN', 'CALLBACKS_UPDATED_NN',
            'callbacks_event_type_nn', 'callbacks_payload_nn', 'callbacks_created_nn', 'callbacks_updated_nn',
            'MESSAGES_EVENT_TYPE_NN', 'MESSAGES_PAYLOAD_NN', 'MESSAGES_CREATED_NN', 'MESSAGES_UPDATED_NN',
            'messages_event_type_nn', 'messages_payload_nn', 'messages_created_nn', 'messages_updated_nn',
        ):
            try_drop_constraint(chk, 'messages')

        create_primary_key('messages_pk', 'messages', ['id'])
        create_check_constraint('messages_event_type_nn', 'messages', 'event_type is not null')
        create_check_constraint('messages_payload_nn', 'messages', 'payload is not null')
        create_check_constraint('messages_created_nn', 'messages', 'created_at is not null')
        create_check_constraint('messages_updated_nn', 'messages', 'updated_at is not null')

    elif is_current_dialect('postgresql'):
        # Drop PK on source (callbacks) if present, then rename to messages.
        drop_current_primary_key('callbacks')
        for pk_name in ('CALLBACKS_PK', 'callbacks_pk', 'callbacks_pkey', 'PRIMARY'):
            try_drop_constraint(pk_name, 'callbacks')

        rename_table('callbacks', 'messages', schema=schema)

        # Idempotency: remove any leftover constraints with either legacy or target names.
        for chk in (
            'CALLBACKS_EVENT_TYPE_NN', 'CALLBACKS_PAYLOAD_NN', 'CALLBACKS_CREATED_NN', 'CALLBACKS_UPDATED_NN',
            'callbacks_event_type_nn', 'callbacks_payload_nn', 'callbacks_created_nn', 'callbacks_updated_nn',
            'MESSAGES_EVENT_TYPE_NN', 'MESSAGES_PAYLOAD_NN', 'MESSAGES_CREATED_NN', 'MESSAGES_UPDATED_NN',
            'messages_event_type_nn', 'messages_payload_nn', 'messages_created_nn', 'messages_updated_nn',
        ):
            try_drop_constraint(chk, 'messages')

        create_primary_key('messages_pk', 'messages', ['id'])
        create_check_constraint('messages_event_type_nn', 'messages', 'event_type is not null')
        create_check_constraint('messages_payload_nn', 'messages', 'payload is not null')
        create_check_constraint('messages_created_nn', 'messages', 'created_at is not null')
        create_check_constraint('messages_updated_nn', 'messages', 'updated_at is not null')

    elif is_current_dialect('mysql'):
        drop_constraint('callbacks_pk', 'callbacks', type_='primary')
        rename_table('callbacks', 'messages', schema=schema)
        create_primary_key('messages_pk', 'messages', ['id'])
        create_check_constraint('messages_event_type_nn', 'messages', 'event_type is not null')
        create_check_constraint('messages_payload_nn', 'messages', 'payload is not null')
        create_check_constraint('messages_created_nn', 'messages', 'created_at is not null')
        create_check_constraint('messages_updated_nn', 'messages', 'updated_at is not null')


def downgrade():
    '''
    Downgrade the database to the previous revision
    '''
    schema = get_effective_schema()
    messages_table = qualify_table('messages', schema)

    if is_current_dialect('oracle'):
        # Drop target-name variants on messages before renaming.
        for chk in (
            'MESSAGES_EVENT_TYPE_NN', 'MESSAGES_PAYLOAD_NN', 'MESSAGES_CREATED_NN', 'MESSAGES_UPDATED_NN',
            'messages_event_type_nn', 'messages_payload_nn', 'messages_created_nn', 'messages_updated_nn',
        ):
            try_drop_constraint(chk, 'messages')

        drop_current_primary_key('messages')
        for pk_name in ('MESSAGES_PK', 'messages_pk', 'messages_pkey', 'PRIMARY'):
            try_drop_constraint(pk_name, 'messages')

        rename_table('messages', 'callbacks')

        # After rename, make sure no stray constraints remain under any name.
        for chk in (
            'CALLBACKS_EVENT_TYPE_NN', 'CALLBACKS_PAYLOAD_NN', 'CALLBACKS_CREATED_NN', 'CALLBACKS_UPDATED_NN',
            'callbacks_event_type_nn', 'callbacks_payload_nn', 'callbacks_created_nn', 'callbacks_updated_nn',
            'MESSAGES_EVENT_TYPE_NN', 'MESSAGES_PAYLOAD_NN', 'MESSAGES_CREATED_NN', 'MESSAGES_UPDATED_NN',
            'messages_event_type_nn', 'messages_payload_nn', 'messages_created_nn', 'messages_updated_nn',
        ):
            try_drop_constraint(chk, 'callbacks')

        create_primary_key('CALLBACKS_PK', 'callbacks', ['id'])
        create_check_constraint('CALLBACKS_EVENT_TYPE_NN', 'callbacks', 'event_type is not null')
        create_check_constraint('CALLBACKS_PAYLOAD_NN', 'callbacks', 'payload is not null')
        create_check_constraint('CALLBACKS_CREATED_NN', 'callbacks', 'created_at is not null')
        create_check_constraint('CALLBACKS_UPDATED_NN', 'callbacks', 'updated_at is not null')

    elif is_current_dialect('postgresql'):
        # Drop target-name variants on messages before renaming.
        for chk in (
            'MESSAGES_EVENT_TYPE_NN', 'MESSAGES_PAYLOAD_NN', 'MESSAGES_CREATED_NN', 'MESSAGES_UPDATED_NN',
            'messages_event_type_nn', 'messages_payload_nn', 'messages_created_nn', 'messages_updated_nn',
        ):
            try_drop_constraint(chk, 'messages')

        drop_current_primary_key('messages')
        for pk_name in ('MESSAGES_PK', 'messages_pk', 'messages_pkey', 'PRIMARY'):
            try_drop_constraint(pk_name, 'messages')

        rename_table('messages', 'callbacks', schema=schema)

        # After rename, make sure no stray constraints remain under any name.
        for chk in (
            'CALLBACKS_EVENT_TYPE_NN', 'CALLBACKS_PAYLOAD_NN', 'CALLBACKS_CREATED_NN', 'CALLBACKS_UPDATED_NN',
            'callbacks_event_type_nn', 'callbacks_payload_nn', 'callbacks_created_nn', 'callbacks_updated_nn',
            'MESSAGES_EVENT_TYPE_NN', 'MESSAGES_PAYLOAD_NN', 'MESSAGES_CREATED_NN', 'MESSAGES_UPDATED_NN',
            'messages_event_type_nn', 'messages_payload_nn', 'messages_created_nn', 'messages_updated_nn',
        ):
            try_drop_constraint(chk, 'callbacks')

        create_primary_key('CALLBACKS_PK', 'callbacks', ['id'])
        create_check_constraint('CALLBACKS_EVENT_TYPE_NN', 'callbacks', 'event_type is not null')
        create_check_constraint('CALLBACKS_PAYLOAD_NN', 'callbacks', 'payload is not null')
        create_check_constraint('CALLBACKS_CREATED_NN', 'callbacks', 'created_at is not null')
        create_check_constraint('CALLBACKS_UPDATED_NN', 'callbacks', 'updated_at is not null')

    elif is_current_dialect('mysql'):
        op.execute(f'ALTER TABLE {messages_table} DROP CHECK MESSAGES_EVENT_TYPE_NN')
        op.execute(f'ALTER TABLE {messages_table} DROP CHECK MESSAGES_PAYLOAD_NN')
        op.execute(f'ALTER TABLE {messages_table} DROP CHECK MESSAGES_CREATED_NN')
        op.execute(f'ALTER TABLE {messages_table} DROP CHECK MESSAGES_UPDATED_NN')
        drop_constraint('messages_pk', 'messages', type_='primary')
        rename_table('messages', 'callbacks', schema=schema)
        create_primary_key('callbacks_pk', 'callbacks', ['id'])
        create_check_constraint('callbacks_event_type_nn', 'callbacks', 'event_type is not null')
        create_check_constraint('callbacks_payload_nn', 'callbacks', 'payload is not null')
        create_check_constraint('callbacks_created_nn', 'callbacks', 'created_at is not null')
        create_check_constraint('callbacks_updated_nn', 'callbacks', 'updated_at is not null')
