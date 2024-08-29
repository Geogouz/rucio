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

import json as json_lib
import logging
import operator
from typing import TYPE_CHECKING, Any

from sqlalchemy import and_, select
from sqlalchemy.exc import DataError, MultipleResultsFound, NoResultFound

from rucio.common.exception import DatabaseException, DataIdentifierNotFound, InvalidMetadata, InvalidObject, KeyNotFound, RucioException
from rucio.core.did_meta_plugins.did_meta_plugin_interface import DidMetaPlugin
from rucio.core.did_meta_plugins.filter_engine import FilterEngine
from rucio.db.sqla import models
from rucio.db.sqla.constants import DIDType
from rucio.db.sqla.session import read_session, stream_session, transactional_session
from rucio.db.sqla.util import json_implemented

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from rucio.common.types import InternalScope, LoggerFunction


class StructuredJSONDidMeta(DidMetaPlugin):
    """
    A plugin to store DID metadata on a table on the relational database, using JSON blobs
    """

    def __init__(self):
        super(StructuredJSONDidMeta, self).__init__()
        self.plugin_name = "STRUCTURED_JSON"

    @read_session
    def get_metadata(
            self,
            scope: str,
            name: str,
            *,
            session: "Session",
            logger: "LoggerFunction" = logging.log
    ) -> dict[str, Any]:
        """
        Get structured metadata for a data identifier.
        :param scope: The scope name.
        :param name: The data identifier name.
        :param session: The database session in use.
        :param logger: Optional logger that can be passed from the calling daemons or servers.
        :returns: The structured metadata as a dictionary.
        :raises NotImplementedError: If JSON is not implemented for the current database.
        :raises DataIdentifierNotFound: If the requested scope and name are not found.
        :raises InvalidMetadata: If no structured metadata exists for the available scope and name.
        :raises DatabaseException: If multiple JSON metadata entries are found for the given scope and name.
        :raises InvalidObject: If the stored JSON data is invalid.
        :raises RucioException: For any other unexpected errors during metadata retrieval.
        """
        if not json_implemented(session=session):
            # Raise an UnsupportedOperation exception if JSON is not implemented in the DB.
            raise NotImplementedError()

        try:
            # TODO: Do we need to check if the DID exists?

            # Get the DidMeta row entry
            stmt = select(
                models.DidMeta
            ).where(
                and_(models.DidMeta.scope == scope,
                     models.DidMeta.name == name)
            )
            did_meta_row_entry = session.execute(stmt).scalar_one_or_none()

            # Raise a DataIdentifierNotFound exception if the DidMeta entry of the requested DID does not exist.
            if did_meta_row_entry is None:
                raise DataIdentifierNotFound()

            # Raise an InvalidMetadata exception if no structured metadata exist for this available DidMeta entry.
            if did_meta_row_entry.structured_meta is None:
                # TODO: Decide whether we want to accept null as a valid value
                raise InvalidMetadata()

            meta = getattr(did_meta_row_entry, 'structured_meta')
            return meta if isinstance(meta, dict) else json_lib.loads(meta)

        except NotImplementedError:
            error_msg = (f"JSON not implemented for this database "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.ERROR, error_msg)
            raise NotImplementedError(error_msg)
        except DataIdentifierNotFound:
            error_msg = (f"Attempted to get metadata for non-existent DID '{scope}:{name}' in DidMeta table "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.WARNING, error_msg)
            raise DataIdentifierNotFound(error_msg)
        except InvalidMetadata:
            error_msg = (f"No structured metadata found for available DID '{scope}:{name}' "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.WARNING, error_msg)
            raise InvalidMetadata(error_msg)
        except MultipleResultsFound:
            error_msg = (f"Data integrity error: Multiple entries found for DID '{scope}:{name}' "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.CRITICAL, error_msg)
            raise DatabaseException(error_msg)
        except json_lib.JSONDecodeError as e:
            error_msg = (f"Invalid JSON data found in the database "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise InvalidObject(error_msg)
        except Exception as e:
            error_msg = (f"An unexpected error occurred while retrieving structured metadata "
                         f"(Used plugin: '{self.plugin_name}')")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise RucioException(error_msg)

    @transactional_session
    def set_metadata(
            self,
            scope: str,
            name: str,
            key: str,
            value: dict[str, Any],
            recursive: bool = False,
            *,
            session: "Session",
            logger: "LoggerFunction" = logging.log
    ) -> None:
        """
        Set structured metadata for a data identifier.
        :param scope: The scope name.
        :param name: The data identifier name.
        :param key: The reserved key (which should be '{}' for identifying the StructuredJSONDidMeta plugin).
        :param value: The metadata to set, as a dictionary.
        :param recursive: Option to propagate the metadata change.
        :param session: The database session in use.
        :param logger: Optional logger that can be passed from the calling daemons or servers.
        :raises NotImplementedError: If JSON is not implemented for the current database.
        :raises DataIdentifierNotFound: If the specified DID doesn't exist.
        :raises DatabaseException: If multiple DID entries are found for the given scope and name.
        :raises InvalidObject: If the provided metadata is not a valid JSON object.
        :raises RucioException: For any other unexpected errors during metadata setting.
        """
        if not json_implemented(session=session):
            # Raise an UnsupportedOperation exception if JSON is not implemented in the DB.
            raise NotImplementedError()

        try:
            # TODO: Do we need this part?
            # First, check if the DID exists
            stmt = select(
                models.DataIdentifier
            ).where(
                and_(models.DataIdentifier.scope == scope,
                     models.DataIdentifier.name == name)
            )

            # Raise a DataIdentifierNotFound exception if the requested DID does not exist.
            if session.execute(stmt).one_or_none() is None:
                raise DataIdentifierNotFound()

            # Then, get the DidMeta row entry
            stmt = select(
                models.DidMeta
            ).where(
                and_(models.DidMeta.scope == scope,
                     models.DidMeta.name == name)
            )
            did_meta_row_entry = session.execute(stmt).scalar_one_or_none()

            # If the DidMeta entry of the requested DID does not exist, create it.
            if did_meta_row_entry is None:
                did_meta_row_entry = models.DidMeta(scope=scope, name=name)
                did_meta_row_entry.save(session=session, flush=False)

            did_meta_row_entry.structured_meta = None
            session.flush()

            # Replace the metadata TODO: decide best strategy for already existing structures
            if session.bind.dialect.name in ['oracle', 'sqlite']:
                did_meta_row_entry.structured_meta = json_lib.dumps(value)
            else:
                did_meta_row_entry.structured_meta = value

            session.flush()

            # logger(logging.DEBUG, f"Structured metadata set for DID '{scope}:{name}'")
        except NotImplementedError:
            error_msg = (f"JSON not implemented for this database "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.ERROR, error_msg)
            raise NotImplementedError(error_msg)
        except DataIdentifierNotFound:
            error_msg = (f"Attempted to set metadata for non-existent DID '{scope}:{name}' in DidMeta table "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.WARNING, error_msg)
            raise DataIdentifierNotFound(error_msg)
        except MultipleResultsFound:
            error_msg = (f"Data integrity error: Multiple entries found for DID '{scope}:{name}' "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.CRITICAL, error_msg)
            raise DatabaseException(error_msg)
        except json_lib.JSONDecodeError as e:
            error_msg = (f"Invalid JSON data provided "
                         f"(Used plugin: '{self.plugin_name}').")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise InvalidObject(error_msg)
        except Exception as e:
            error_msg = (f"An unexpected error occurred while setting structured metadata "
                         f"(Used plugin: '{self.plugin_name}')")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise RucioException(error_msg)

    @transactional_session
    def set_metadata_bulk(
            self,
            scope: "InternalScope",
            name: str,
            metadata: dict[str, Any],
            *,
            session: "Session",
            logger: "LoggerFunction" = logging.log
    ) -> None:
        """
        Set structured metadata for a data identifier.
        :param scope: The scope name.
        :param name: The data identifier name.
        :param metadata: The metadata to set. TODO: Decide whether bulk function for StructuredJSONDidMeta makes sense.
        :param session: The database session in use.
        :param logger: Optional logger that can be passed from the calling daemons or servers.
        :raises KeyNotFound: If the '{}' key (being specific to StructuredJSONDidMeta) is not present in the metadata.
        """
        if '{}' in metadata:
            return self.set_metadata(scope=scope, name=name, key='{}', value=metadata['{}'])
        else:
            raise KeyNotFound('{}')

    @transactional_session
    def delete_metadata(self, scope, name, key, *, session: "Session"):
        """
        Delete a key from the metadata column

        :param scope: the scope of did
        :param name: the name of the did
        :param key: the key to be deleted
        :param session: The database session in use.
        """
        if not json_implemented(session=session):
            raise NotImplementedError

        try:
            stmt = select(
                models.DidMeta
            ).where(
                and_(models.DidMeta.scope == scope,
                     models.DidMeta.name == name)
            )
            row = session.execute(stmt).scalar_one()
            existing_meta = getattr(row, 'meta')
            # Oracle returns a string instead of a dict
            if session.bind.dialect.name in ['oracle', 'sqlite'] and existing_meta is not None:
                existing_meta = json_lib.loads(existing_meta)

            if key not in existing_meta:
                raise KeyNotFound(key)

            existing_meta.pop(key, None)

            row.meta = None
            session.flush()

            # Oracle insert takes a string as input
            if session.bind.dialect.name in ['oracle', 'sqlite']:
                existing_meta = json_lib.dumps(existing_meta)

            row.meta = existing_meta
        except NoResultFound:
            raise DataIdentifierNotFound(f"Key not found for data identifier '{scope}:{name}'")

    @stream_session
    def list_dids(self, scope, filters, did_type='collection', ignore_case=False, limit=None,
                  offset=None, long=False, recursive=False, ignore_dids=None, *, session: "Session"):
        if not json_implemented(session=session):
            raise NotImplementedError

        if not ignore_dids:
            ignore_dids = set()

        # backwards compatibility for filters as single {}.
        if isinstance(filters, dict):
            filters = [filters]

        # instantiate fe and create sqla query, note that coercion to a model keyword
        # is not appropriate here as the filter words are stored in a single json column.
        fe = FilterEngine(filters, model_class=models.DidMeta, strict_coerce=False)
        stmt = fe.create_sqla_query(
            additional_model_attributes=[
                models.DidMeta.scope,
                models.DidMeta.name
            ], additional_filters=[
                (models.DidMeta.scope, operator.eq, scope)
            ],
            json_column=models.DidMeta.meta,
            session=session
        )

        if limit:
            stmt = stmt.limit(
                limit
            )
        if recursive:
            from rucio.core.did import list_content

            # Get attached DIDs and save in list because query has to be finished before starting a new one in the recursion
            collections_content = []
            for did in session.execute(stmt).yield_per(100):
                if (did.did_type == DIDType.CONTAINER or did.did_type == DIDType.DATASET):
                    collections_content += [d for d in list_content(scope=did.scope, name=did.name)]

            # Replace any name filtering with recursed DID names.
            for did in collections_content:
                for or_group in filters:
                    or_group['name'] = did['name']
                for result in self.list_dids(scope=did['scope'], filters=filters, recursive=True, did_type=did_type, limit=limit, offset=offset,
                                             long=long, ignore_dids=ignore_dids, session=session):
                    yield result

        try:
            for did in session.execute(stmt).yield_per(5):                  # don't unpack this as it makes it dependent on query return order!
                if long:
                    did_full = "{}:{}".format(did.scope, did.name)
                    if did_full not in ignore_dids:         # concatenating results of OR clauses may contain duplicate DIDs if query result sets not mutually exclusive.
                        ignore_dids.add(did_full)
                        yield {
                            'scope': did.scope,
                            'name': did.name,
                            'did_type': None,               # not available with JSON plugin
                            'bytes': None,                  # not available with JSON plugin
                            'length': None                  # not available with JSON plugin
                        }
                else:
                    did_full = "{}:{}".format(did.scope, did.name)
                    if did_full not in ignore_dids:         # concatenating results of OR clauses may contain duplicate DIDs if query result sets not mutually exclusive.
                        ignore_dids.add(did_full)
                        yield did.name
        except DataError as e:
            raise InvalidMetadata("Database query failed: {}. This can be raised when the datatype of a key is inconsistent between DIDs.".format(e))

    @read_session
    def manages_key(self, key, *, session: "Session"):
        return json_implemented(session=session) and key == '{}'

    def get_plugin_name(self):
        """
        Returns a unique identifier for this plugin. This can be later used for filtering down results to this plugin only.
        :returns: The name of the plugin.
        """
        return self.plugin_name
