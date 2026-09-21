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

import logging
from unittest.mock import patch

import pytest
from sqlalchemy import text

from rucio.common.exception import InputValidationError
from rucio.db.sqla.session import NullPool, QueuePool, SingletonThreadPool, _get_engine_poolclass, get_session, read_session, stream_session, transactional_session


def test_db_connection():
    """ DB (CORE): Test db connection """
    session = get_session()
    if session.bind.dialect.name == 'oracle':
        session.execute(text('select 1 from dual'))
    else:
        session.execute(text('select 1'))
    session.close()


def test_config_poolclass():
    assert _get_engine_poolclass('nullpool') is NullPool
    assert _get_engine_poolclass('queuepool') is QueuePool
    assert _get_engine_poolclass('singletonthreadpool') is SingletonThreadPool

    with pytest.raises(InputValidationError, match='Unknown poolclass: unknown'):
        _get_engine_poolclass('unknown')


@pytest.mark.parametrize('decorator', [read_session, transactional_session])
def test_implicit_session_warning(decorator, db_session, caplog, monkeypatch):
    monkeypatch.setattr('rucio.db.sqla.session._IMPLICIT_SESSION_WARNED', set())

    @decorator
    def first(*, session):
        return session

    @decorator
    def second(*, session):
        return session

    with caplog.at_level(logging.WARNING, logger='rucio.db.sqla.session'):
        for index, function in enumerate((first, second)):
            assert function(session=db_session) is db_session
            assert len(caplog.records) == index

            function()
            function()
            function(session=None)
            assert function(session=db_session) is db_session

            assert len(caplog.records) == index + 1
            record = caplog.records[index]
            name = f'{function.__module__}.{function.__qualname__}'
            assert record.name == 'rucio.db.sqla.session'
            assert record.levelno == logging.WARNING
            assert record.getMessage() == (
                f'{name} was called without a session; session will become a required parameter '
                '(https://github.com/rucio/rucio/issues/6989)'
            )
            assert record.stack_info is not None
            assert 'test_implicit_session_warning' in record.stack_info


def test_stream_session_warning_on_iteration(db_session, caplog, monkeypatch):
    monkeypatch.setattr('rucio.db.sqla.session._IMPLICIT_SESSION_WARNED', set())

    @stream_session
    def stream(*, session):
        yield session

    with caplog.at_level(logging.WARNING, logger='rucio.db.sqla.session'):
        assert list(stream(session=db_session)) == [db_session]
        result = stream()
        assert not caplog.records

        list(result)
        list(stream())
        list(stream(session=None))
        assert list(stream(session=db_session)) == [db_session]

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.name == 'rucio.db.sqla.session'
    assert record.levelno == logging.WARNING
    assert f'{stream.__module__}.{stream.__qualname__}' in record.getMessage()
    assert record.stack_info is not None
    assert 'test_stream_session_warning_on_iteration' in record.stack_info


@pytest.mark.noparallel(reason='Changes an internal method of MethodView.')
def test_pooloverload():
    """ DB (WEB): Test response to a DatabaseException due to Pool Overflow """
    from rucio.common.exception import DatabaseException
    from rucio.web.rest.flaskapi.v1.ping import Ping

    # Create a new ErrorHandlingMethodView as_view
    ping_view = Ping.as_view('ping')

    # specification for the mock we create to replace flask.request
    # without specifying this, _is_async_obj is run which triggers flask RuntimeError
    class T:
        method = 'replacement string'

    patch_flask = patch('flask.request', spec=T)

    patch_getheaders = patch('rucio.web.rest.flaskapi.v1.ping.Ping.get_headers')
    patch_dispatch = patch(
        'flask.views.MethodView.dispatch_request',
        side_effect=DatabaseException("QueuePool Exception Somehow")
    )

    patch_flask.start()
    patch_getheaders.start()
    patch_dispatch.start()

    response = ping_view.view_class.dispatch_request(ping_view.view_class)
    # Assert the correct error is raised.
    assert ('Currently there are too many requests for the Rucio servers to handle. '
            'Please try again in a few minutes.' in response.data.decode())

    patch.stopall()
