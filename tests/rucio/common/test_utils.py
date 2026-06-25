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

import base64
from io import StringIO

import pytest

from rucio.common.utils import ScopeExtractionAlgorithms, _encode_params_as_url_query_string, build_url, invert_dict, ssh_sign


class TestUtils:
    def test_invert_dict(self):
        input_dict = {'a': 1, 'b': 2, 'c': 3}
        inverse_dict = {1: 'a', 2: 'b', 3: 'c'}

        assert invert_dict(input_dict) == inverse_dict

    @pytest.mark.parametrize(
        'input_url, path, params, doseq, expected_url',
        [
            ('https://test:443', None, None, False, 'https://test:443'),
            ('https://test:443', 'test', None, False, 'https://test:443/test'),
            ('https://test:443', 'test', None, False, 'https://test:443/test'),
            ('https://test:443', 'test', 'testparam=1', False, 'https://test:443/test?testparam%3D1'),
            ('https://test:443', None, 'testparam=1', False, 'https://test:443?testparam%3D1'),
            ('https://test:443', 'test', {'a': 1, 'b': 2}, False, 'https://test:443/test?a=1&b=2'),
            ('https://test:443', None, {'a': 1, 'b': 2}, False, 'https://test:443?a=1&b=2'),
            ('https://test:443', 'test', [('a', 1), ('b', 2)], True, 'https://test:443/test?a=1&b=2'),
        ]
    )
    def test_build_url(self, input_url, path, params, doseq, expected_url):
        assert build_url(input_url, path, params, doseq) == expected_url

    @pytest.mark.parametrize(
        'params, doseq, expected_query_string',
        [
            ('testparam=1', False, '?testparam%3D1'),
            ({'a': 1, 'b': 2}, False, '?a=1&b=2'),
            ([('a', 1), ('b', 2)], True, '?a=1&b=2')
        ]
    )
    def test_encode_params_as_url_query_string(self, params, doseq, expected_query_string):
        assert _encode_params_as_url_query_string(params, doseq) == expected_query_string

    def test_ssh_sign_uses_rsa_sha2_algorithm(self, monkeypatch):
        paramiko = pytest.importorskip("paramiko")
        key = paramiko.RSAKey.generate(2048)
        private_key = StringIO()
        key.write_private_key(private_key)
        monkeypatch.setattr(paramiko.RSAKey, "HASHES", {name: hash_algorithm for name, hash_algorithm in paramiko.RSAKey.HASHES.items() if name != 'ssh-rsa'})

        signature = ssh_sign(private_key.getvalue(), 'challenge-token')

        signature_message = paramiko.Message(base64.b64decode(signature))
        assert signature_message.get_text() == 'rsa-sha2-256'
        signature_message.rewind()
        assert key.verify_ssh_sig(b'challenge-token', signature_message)

    @pytest.mark.parametrize(
        'did, scope, name',
        [
            ("scope:name", "scope", "name"),
            ("scope:/this/is/a/path", "scope", "/this/is/a/path"),
            ("scope:/this/is/a/path/", "scope", "/this/is/a/path/"),
            ("scope:no/slash/at/start", "scope", "no/slash/at/start"),
            ("scope:no/slash/at/start/", "scope", "no/slash/at/start/"),
            ("scope:/", "scope", "/"),
            ("scope://", "scope", "//"),
            ("scope:/path/with//duplicated///slash", "scope", "/path/with//duplicated///slash"),
        ]
    )
    def test_default_scope_extraction_algorithm(self, did, scope, name):
        assert ScopeExtractionAlgorithms.extract_scope_default(did=did, scopes=None) == (scope, name)
