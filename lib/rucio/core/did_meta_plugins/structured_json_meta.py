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
import logging  # TODO[DX]: Ensure we are not storing mb's of values..
import operator
from datetime import datetime
from typing import TYPE_CHECKING, Any, Union

from sqlalchemy import and_, select, text, update
from sqlalchemy.exc import DataError, MultipleResultsFound, NoResultFound
from sqlalchemy.sql import func

from rucio.common.exception import DatabaseException, DataIdentifierNotFound, InvalidMetadata, InvalidObject, KeyNotFound, RucioException
from rucio.core.did_meta_plugins.did_meta_plugin_interface import DidMetaPlugin
from rucio.core.did_meta_plugins.filter_engine import FilterEngine
from rucio.db.sqla import models
from rucio.db.sqla.constants import DIDType
from rucio.db.sqla.session import read_session, stream_session, transactional_session
from rucio.db.sqla.util import json_implemented

if TYPE_CHECKING:
    from typing import Optional

    from sqlalchemy.orm import Session

    from rucio.common.types import InternalScope, LoggerFunction


class StructuredJSONDidMeta(DidMetaPlugin):
    """
    A plugin to store structure metadata for a DID in the 'meta' table on the relational database, using JSON blobs.
    """

    def __init__(self):
        super(StructuredJSONDidMeta, self).__init__()

        self._plugin_name = "STRUCTURED_JSON"

    @read_session
    def get_metadata(
            self,
            scope: "InternalScope",
            name: str,
            *,
            session: "Session",
            logger: "LoggerFunction" = logging.log
    ) -> dict[str, Any]:
        """
        Returns a dictionary holding all structured metadata available for a given DID.

        :param scope: The scope of the DID.
        :param name: The data identifier name.
        :param session: The database session in use.
        :param logger: A logger that can be passed from the calling daemons or servers.
        :returns: The structured metadata as a dictionary.
        :raises NotImplementedError: If JSON is not implemented for the current database.
        :raises DataIdentifierNotFound: If the requested scope and name are not found.
        :raises MultipleResultsFound: If multiple entries are found for the given scope and name.
        :raises RucioException: For any other unexpected errors during metadata retrieval.
        """
        try:
            if not json_implemented(session=session):
                # Raise an UnsupportedOperation exception if JSON is not implemented in the DB.
                raise NotImplementedError()  # TODO[DX]: Switch to Rucio Exception

            # Verify the DID exists
            did_exists = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).one_or_none()
            if did_exists is None:
                raise DataIdentifierNotFound(f"DID '{scope}:{name}' does not exist.")

            # Get the DidMeta row entry
            did_meta_row_entry = session.query(models.DidMeta).filter_by(scope=scope, name=name).one_or_none()
            if did_meta_row_entry is None:
                # No entry exists in the 'meta' table for the requested DID.
                return {}
            else:
                return self.get_meta_as_dict(did_meta_row_entry.structured_meta or {})

        except NotImplementedError:
            error_msg = (f"JSON not implemented for this database "
                         f"(Used plugin: '{self.name}').")
            logger(logging.ERROR, error_msg)
            raise NotImplementedError(error_msg)
        except DataIdentifierNotFound:
            error_msg = (f"Attempted to get metadata for non-existent DID '{scope}:{name}' in DidMeta table "
                         f"(Used plugin: '{self.name}').")
            logger(logging.WARNING, error_msg)
            raise DataIdentifierNotFound(error_msg)
        except MultipleResultsFound:
            error_msg = (f"Data integrity error: Multiple entries found for DID '{scope}:{name}' "
                         f"(Used plugin: '{self.name}').")
            logger(logging.CRITICAL, error_msg)
            raise DatabaseException(error_msg)
        except Exception as e:
            error_msg = (f"An unexpected error occurred while retrieving structured metadata "
                         f"(Used plugin: '{self.name}')")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise RucioException(error_msg)

    @transactional_session
    def set_metadata(
            self,
            scope: "InternalScope",
            name: str,
            key: str,
            value: str,
            recursive: bool = False,
            *,
            session: "Optional[Session]" = None
    ) -> None:
        pass

    @transactional_session
    def set_metadata_bulk(
            self,
            scope: "InternalScope",
            name: str,
            meta: dict[str, "Any"],
            recursive: bool = False,
            *,
            session: "Optional[Session]" = None
    ) -> None:
        pass

    @transactional_session
    def set_metadata_atomic(
            self,
            scope: "InternalScope",
            name: str,
            key: str,
            value: dict[str, Any],
            recursive: bool = False,  # TODO: implement
            *,
            session: "Session",
            logger: "LoggerFunction" = logging.log
    ) -> None:
        """
        Set a value in a structured metadata JSON at a specified nested path.

        General Principles for constructing the metadata-update target-value path:
            - Keys in Objects: Use the key name at each level to navigate through JSON objects.
            - Indices in Arrays: Use the 0-based index to navigate JSON arrays.
            - Paths are constructed as arrays of strings, with each string representing a key or index.

        Example Path Syntax:
            '{key1,key2,...,keyN}'  -- For objects
            '{key1,index,key2,...}' -- For mixed object/array nesting

        Example 1: Nested Objects
            JSON:
                {
                  "properties": {
                    "dfe:flood_detection": {
                      "observed_water_bodies": null
                    }
                  }
                }
            Goal: Get the path for updating the "null" value of "observed_water_bodies".
            Path: '{properties,dfe:flood_detection,observed_water_bodies}'

        Example 2: Mixed Objects and Arrays
            JSON:
                {
                  "flood_data": [
                    {"region": "A", "status": "active"},
                    {"region": "B", "status": "inactive"}
                  ]
                }
            Goal: Get the path for updating the "inactive" value of "status" in the second array element (i.e. index 1).
            Path: '{flood_data,1,status}'

        :param scope: The scope of the DID.
        :param name: The data identifier name.
        :param key: The key path using comma notation (e.g. "a,b,c" for {"a": {"b": {"c": value}}}).
        :param value: The value (valid JSON structure) to set at the specified key path.
        :param recursive: Instruction to propagate the metadata change recursively to content (False by default).
        :param session: The database session in use.
        :param logger: (optional) A logger that can be passed from the calling daemons or servers.
        :raises NotImplementedError: If JSON is not implemented for the current database.
        :raises DataIdentifierNotFound: If the specified DID doesn't exist.
        :raises MultipleResultsFound: If multiple entries are found for the given scope and name.
        :raises RucioException: For any other unexpected errors during metadata setting.

        Example:
            set_metadata_atomic(scope, name, "ab,cd,ef", value)
            # Results in: {"ab": {"cd": {"ef": value}}}
        """
        if not json_implemented(session=session):
            # Raise an UnsupportedOperation exception if JSON is not implemented in the DB.
            raise NotImplementedError("JSON support is not implemented for this database.")  # TODO: Change to Rucio exception

        # Explicit check for an entirely empty key
        if not key.strip():
            raise InvalidObject(f"Invalid key path: '{key}'. Key path must not be empty.")

        # Validate key path for empty segments
        keys = key.split(",")
        if any(not part.strip() for part in keys):
            raise ValueError(f"Invalid key path: '{key}'. Key path must not contain empty segments.")

        try:
            # Verify the DID exists
            did_exists = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).one_or_none()
            if did_exists is None:
                raise DataIdentifierNotFound(f"DID '{scope}:{name}' does not exist.")

            # Fetch the DidMeta entry
            did_meta_row_entry = session.query(models.DidMeta).filter_by(scope=scope, name=name).one_or_none()

            if did_meta_row_entry is None:
                # Initialize a new metadata structure with the nested path
                new_meta = {}
                current = new_meta
                for part in keys[:-1]:
                    current[part] = {}
                    current = current[part]
                current[keys[-1]] = value

                did_meta_row_entry = models.DidMeta(scope=scope, name=name, structured_meta=new_meta)
                session.add(did_meta_row_entry)
            else:
                if session.bind.dialect.name == 'postgresql':
                    # TODO: Existing metadata are assumed to be valid. Add metadata validation for the requested update.
                    # Construct the SQL statement to set the nested value
                    stmt = (
                        update(models.DidMeta)
                        .where(and_(models.DidMeta.scope == scope, models.DidMeta.name == name))
                        .values(
                            {
                                models.DidMeta.structured_meta: text(
                                    "jsonb_set(coalesce(structured_meta, '{}'::jsonb), ARRAY['" + key + "'], '" + json_lib.dumps(
                                        value) + "'::jsonb, true)"
                                ),
                                models.DidMeta.updated_at: datetime.utcnow()
                            }
                        )
                    )
                    session.execute(stmt)
                else:
                    # Fallback for non-PostgreSQL databases: Update the JSON content manually
                    # TODO: Optimize for Oracle too
                    # TODO: Metadata are being retrieved first, before the update (not good for big metadata content).
                    # TODO: Existing metadata are assumed to be valid. Add metadata validation for the requested update.
                    # Load existing metadata
                    existing_meta = self.get_meta_as_dict(did_meta_row_entry.structured_meta or {})
                    current = existing_meta
                    for part in keys[:-1]:
                        if part not in current or not isinstance(current[part], dict):
                            current[part] = {}
                        current = current[part]
                    current[keys[-1]] = value

                    did_meta_row_entry.structured_meta = json_lib.dumps(existing_meta)

            session.flush()
            # logger(logging.DEBUG, f"Structured metadata set for DID '{scope}:{name}'")

        except NotImplementedError:
            error_msg = (f"JSON not implemented for this database "
                         f"(Used plugin: '{self.name}').")
            logger(logging.ERROR, error_msg)
            raise NotImplementedError(error_msg)
        except DataIdentifierNotFound:
            error_msg = (f"Attempted to set metadata for non-existent DID '{scope}:{name}' in DidMeta table "
                         f"(Used plugin: '{self.name}').")
            logger(logging.WARNING, error_msg)
            raise DataIdentifierNotFound(error_msg)
        except MultipleResultsFound:
            error_msg = (f"Data integrity error: Multiple entries found for DID '{scope}:{name}' "
                         f"(Used plugin: '{self.name}').")
            logger(logging.CRITICAL, error_msg)
            raise DatabaseException(error_msg)
        except Exception as e:
            error_msg = (f"An unexpected error occurred while setting structured metadata "
                         f"(Used plugin: '{self.name}')")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise RucioException(error_msg)

    @transactional_session
    def set_metadata_bulk_atomic(
            self,
            scope: "InternalScope",
            name: str,
            meta: dict[str, Any],
            recursive: bool = False,  # TODO: implement
            *,
            session: "Session",
            logger: "LoggerFunction" = logging.log
    ) -> None:
        """
        Add metadata entries (key-value pairs) to a DID atomically (single plugin handler via a single DB transaction),
        and with option to perform that recursively to all its children.

        The behavior for merging metadata follows these principles:
        - The default strategy merges first-level keys from the new metadata into the existing metadata.
        - Overlapping keys are overwritten entirely, including nested structures.
        - Non-overlapping keys from both the existing and new metadata are retained.
        # TODO[DX]: Add optional strategies (e.g. for recursive updates)

        :param scope: The scope of the DID.
        :param name: The data identifier name.
        :param meta: A dictionary containing the metadata to set.
        :param recursive: Instruction to propagate the metadata change recursively to content (False by default).
        :param session: The database session in use.
        :param logger: A logger that can be passed from the calling daemons or servers.
        :raises NotImplementedError: If JSON is not implemented for the current database.
        :raises DataIdentifierNotFound: If the specified DID doesn't exist.
        :raises MultipleResultsFound: If multiple entries are found for the given scope and name.
        :raises RucioException: For any other unexpected errors during metadata setting.
        """
        try:
            if not json_implemented(session=session):
                # Raise an UnsupportedOperation exception if JSON is not implemented in the DB.
                raise NotImplementedError()  # TODO[DX]: Switch to Rucio Exception

            # Verify the DID exists
            did_exists = session.query(models.DataIdentifier).filter_by(scope=scope, name=name).one_or_none()
            if did_exists is None:
                raise DataIdentifierNotFound(f"DID '{scope}:{name}' does not exist.")

            # Get the DidMeta row entry
            did_meta_row_entry = session.query(models.DidMeta).filter_by(scope=scope, name=name).one_or_none()
            if did_meta_row_entry is None:
                # Create an entry in the 'meta' table for the requested DID since none exists.
                did_meta_row_entry = models.DidMeta(scope=scope, name=name, structured_meta=meta)
                session.add(did_meta_row_entry)
            else:
                if session.bind.dialect.name == 'postgresql':
                    # Update existing metadata
                    # TODO[DX]: Existing metadata are assumed to be valid. Add metadata validation for the requested update.
                    stmt = (
                        update(models.DidMeta)
                        .where(
                            and_(models.DidMeta.scope == scope,
                                 models.DidMeta.name == name)
                        )
                        .values(
                            structured_meta=func.coalesce(
                                models.DidMeta.structured_meta, func.jsonb('{}')
                            ).op('||')(meta)
                        )
                    )
                    session.execute(stmt)
                else:
                    # Fallback for non-PostgreSQL databases: Merge metadata and serialize as JSON string
                    # TODO[DX]: Optimize for Oracle too
                    # TODO[DX]: Metadata are being retrieved first, before the update (not good for big metadata content).
                    # TODO[DX]: Existing metadata are assumed to be valid. Add metadata validation for the requested update.
                    # Load existing metadata
                    existing_meta = self.get_meta_as_dict(did_meta_row_entry.structured_meta or {})
                    updated_meta = {**existing_meta, **meta}

                    # Most databases handle direct updates to JSON fields without requiring intermediate clearing.
                    # Yet, for extreme cases, perform a reset before reassigning a value just to be on the safe side.
                    did_meta_row_entry.structured_meta = None
                    session.flush()

                    did_meta_row_entry.structured_meta = json_lib.dumps(updated_meta)

            session.flush()
            # logger(logging.DEBUG, f"Structured metadata set for DID '{scope}:{name}'")

        except NotImplementedError:
            error_msg = (f"JSON not implemented for this database "
                         f"(Used plugin: '{self.name}').")
            logger(logging.ERROR, error_msg)
            raise NotImplementedError(error_msg)
        except DataIdentifierNotFound:
            error_msg = (f"Attempted to set metadata for non-existent DID '{scope}:{name}' in DidMeta table "
                         f"(Used plugin: '{self.name}').")
            logger(logging.WARNING, error_msg)
            raise DataIdentifierNotFound(error_msg)
        except MultipleResultsFound:
            error_msg = (f"Data integrity error: Multiple entries found for DID '{scope}:{name}' "
                         f"(Used plugin: '{self.name}').")
            logger(logging.CRITICAL, error_msg)
            raise DatabaseException(error_msg)
        except Exception as e:
            error_msg = (f"An unexpected error occurred while setting structured metadata "
                         f"(Used plugin: '{self.name}')")
            logger(logging.ERROR, f"{error_msg}: {str(e)}", exc_info=True)
            raise RucioException(error_msg)

    @transactional_session
    def delete_metadata(
        self,
        scope: "InternalScope",
        name: str,
        key: str,
        *,
        session: "Optional[Session]" = None
    ) -> None:
        """
        Delete a key from the metadata column

        :param scope: The scope of the DID.
        :param name: The data identifier name.
        :param key: The key to be deleted.
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
    def list_dids(
            self,
            scope,
            filters,
            did_type='collection',
            ignore_case=False,
            limit=None,
            offset=None,
            long=False,
            recursive=False,
            ignore_dids=None,
            *,
            session: "Session"
    ):
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
                if did.did_type == DIDType.CONTAINER or did.did_type == DIDType.DATASET:
                    collections_content += [d for d in list_content(scope=did.scope, name=did.name)]

            # Replace any name filtering with recursed DID names.
            for did in collections_content:
                for or_group in filters:
                    or_group['name'] = did['name']
                for result in self.list_dids(scope=did['scope'], filters=filters, recursive=True, did_type=did_type, limit=limit, offset=offset,
                                             long=long, ignore_dids=ignore_dids, session=session):
                    yield result

        try:
            for did in session.execute(stmt).yield_per(5):  # don't unpack this as it makes it dependent on query return order!
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

    def manages_key(
            self,
            key: str,
            *,
            session: "Optional[Session]" = None
    ) -> bool:
        # If set to True, ensure we have the correct methods available to support caller's next commands
        return False

    @read_session
    def supports_metadata_schema(
            self,
            meta: any,
            *,
            session: "Session"
    ) -> bool:
        """
        Validates that this plugin can handle the given metadata schema.

        :param meta: The metadata schema to validate
        :param session: The database session in use.
        :returns: Always True for now: TODO
        """
        return True

    def get_meta_as_dict(
            self,
            data: Union[str, dict[str, Any]]
    ) -> dict:
        """
        Attempts to return the input metadata as a dictionary.

        :param data: A string or dictionary to parse.
        :returns: The input metadata as a dictionary.
        :raises InvalidObject: If the input metadata cannot be converted to a dictionary.
        :raises InvalidMetadata: If metadata are of unexpected type.
        """
        if isinstance(data, dict):
            return data
        elif isinstance(data, str):
            try:
                parsed = json_lib.loads(data)
                if isinstance(parsed, dict):
                    return parsed
                raise InvalidObject("The provided JSON string is valid but does not represent a dictionary.")
            except Exception:
                raise InvalidObject("Failed to decode JSON string")
        raise InvalidMetadata("Input data must be a dictionary or a JSON string.")
