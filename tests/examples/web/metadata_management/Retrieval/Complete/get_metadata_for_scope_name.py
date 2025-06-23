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
import sys
from pathlib import Path

from rucio.common.utils import setup_logger

# Add the project root directory to the Python path to be able to locate rucio libs and scripts
sys.path.append(str(Path(__file__).resolve().parent.parent.parent.parent.parent))
from rucio.dafab_lib import *

LOG = setup_logger(module_name=__name__)


######################################################################
# Test metadata fetching using the 'JSON' plugin
######################################################################
def get_metadata_test_json(scope, name, plugin='JSON'):
    try:
        # Get a client connection
        cl = connection_manager("root-dev", True)
        return cl.get_metadata(scope=scope, name=name, plugin=plugin)
    except Exception as e:
        LOG(logging.WARNING,
            f"An unexpected error of type '{type(e).__name__}' occurred "
            f"while getting all metadata for scope:name '{_scope}:{_name}': {e}\n")

        # Get the full traceback for debugging:
        traceback.print_exc()
        return None
######################################################################


_scope = 'mock'
_name = 'dataset_7e9899f6b4a94fd4872cf6e43c8efdc8'
all_metadata = get_metadata_test_json(scope=_scope, name=_name)
print(f"All metadata for scope:name '{_scope}:{_name}' >> \n{all_metadata}")
# print(f"All metadata for scope:name '{_scope}:{_name}' >> \n{jsn.dumps(all_metadata, indent=2)}")


######################################################################
# Test metadata fetching using the 'DID_COLUMN' plugin
######################################################################
def get_metadata_test_did_column(scope, name):
    try:
        # Get a client connection
        cl = connection_manager("root-dev", True)
        return cl.get_metadata(scope=scope, name=name)
    except Exception as e:
        LOG(logging.WARNING,
            f"An unexpected error of type '{type(e).__name__}' occurred "
            f"while getting all metadata for scope:name '{_scope}:{_name}': {e}\n")

        # Get the full traceback for debugging:
        traceback.print_exc()
        return None
######################################################################

_scope = 'mock'
_name = 'dataset_7e9899f6b4a94fd4872cf6e43c8efdc8'
all_metadata = get_metadata_test_did_column(scope=_scope, name=_name)
print(f"All metadata for scope:name '{_scope}:{_name}' >> \n{all_metadata}")
# print(f"All metadata for scope:name '{_scope}:{_name}' >> \n{jsn.dumps(all_metadata, indent=2)}")
