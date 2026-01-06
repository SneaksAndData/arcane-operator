#!/usr/bin/env bash

# Copyright 2017 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
CODEGEN_PKG=${CODEGEN_PKG:-$(cd "${SCRIPT_ROOT}"; ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

source "${CODEGEN_PKG}/kube_codegen.sh"

THIS_PKG="github.com/SneaksAndData/arcane-operator"

if [ -z "${API_ROOT:-}" ]; then
  API_ROOT="${SCRIPT_ROOT}/pkg/apis"
fi

if [ -z "${GEN_ROOT:-}" ]; then
  GEN_ROOT="${SCRIPT_ROOT}/pkg/generated"
fi

if [ -z "${GEN_PKG:-}" ]; then
  GEN_PKG="pkg/generated"
fi

kube::codegen::gen_helpers \
    --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    "${API_ROOT}"

kube::codegen::gen_client \
    --with-watch \
    --with-applyconfig \
    --output-dir "${GEN_ROOT}" \
    --output-pkg "${THIS_PKG}/${GEN_PKG}" \
    --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
    "${API_ROOT}"
