# ==================================================================================================
# Copyright 2011 Twitter, Inc.
# --------------------------------------------------------------------------------------------------
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this work except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file, or at:
#
#  https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==================================================================================================

create_thrift_libraries(
    base_name = "thrift",
    sources = ["com/twitter/serversets/endpoint.thrift"],
    tags = ["bazel-compatible"],
    generate_languages = [
        "java",
        "scala",
    ],
    provides_java_name = "finagle-serversets-thrift-java",
)

python_thrift_library(
    name = "thrift-python",
    sources = [],
    tags = ["bazel-compatible"],
)

python_thrift_library(
    name = "py-thrift",
    sources = ["com/twitter/serversets/endpoint.thrift"],
    compatibility = ["CPython>=2.7,<4"],
    tags = ["bazel-compatible"],
)
