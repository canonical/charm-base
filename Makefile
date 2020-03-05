# Copyright 2019 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ENV = $(CURDIR)/env
PYTHON3 = $(ENV)/bin/python3

test: $(ENV) raw-test

raw-test: $(ENV) lint
	@python3 -m unittest

lint: quotelint check-copyright
	${PYTHON3} -m autopep8 -r --aggressive --diff --exit-code --exclude env .
	${PYTHON3} -m flake8 --config=.flake8 --exclude env .

quotelint:
	@x=$$(grep -rnH --include \*.py "\\\\[\"']" --exclude-dir env .);                       \
	if [ "$$x" ]; then                                                    \
		echo "Please fix the quoting to avoid spurious backslashes:"; \
		echo "$$x";                                                   \
		exit 1;                                                       \
	fi >&2

check-copyright:
	@x=$$(find . -name env -prune -o -name \*.py -not -empty -type f -print0 | xargs -0 grep -L "^# Copyright"); \
	if [ "$$x" ]; then                                                                       \
		echo "Please add copyright headers to the following files:";                     \
		echo "$$x";                                                                      \
		exit 1;                                                                          \
	fi >&2

$(ENV):
	python3 -m venv $(ENV)
	${PYTHON3} -m pip install -r requirements-test.txt

clean:
	rm -rf $(ENV)

.PHONY: lint test quotelint check-copyright
