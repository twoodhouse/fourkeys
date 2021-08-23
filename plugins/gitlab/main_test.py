# Copyright 2020 Google, LLC.
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
import json

import main
import shared

import mock
import pytest


@pytest.fixture
def client():
    main.app.testing = True
    return main.app.test_client()


def test_not_json(client):
    with pytest.raises(Exception) as e:
        client.post("/", data="foo")

    assert "Expecting JSON payload" in str(e.value)


def test_not_pubsub_message(client):
    with pytest.raises(Exception) as e:
        client.post(
            "/",
            data=json.dumps({"foo": "bar"}),
            headers={"Content-Type": "application/json"},
        )

    assert "Not a valid Pub/Sub Message" in str(e.value)


def test_missing_msg_attributes(client):
    with pytest.raises(Exception) as e:
        client.post(
            "/",
            data=json.dumps({"message": "bar"}),
            headers={"Content-Type": "application/json"},
        )

    assert "Missing pubsub attributes" in str(e.value)


def test_gitlab_event_processed(client):
    headers = {"X-Gitlab-Event": "push", "X-Gitlab-Token": "foo"}
    data = json.dumps({"object_kind": "push",
                       "checkout_sha": "foo",
                       "commits": [{"id": "foo", "timestamp": 2}],
                       }).encode("utf-8")

    pubsub_msg = {
        "message": {
            "data": base64.b64encode(data).decode("utf-8"),
            "attributes": {"headers": json.dumps(headers)},
            "message_id": "foobar",
            "publishTime": 1,
        },
    }

    event = {
        "event_type": "push",
        "id": "foo",
        "metadata": data.decode(),
        "time_created": 2,
        "signature": shared.create_unique_id(pubsub_msg["message"]),
        "msg_id": "foobar",
        "source": "gitlab",
    }

    shared.insert_row_into_bigquery = mock.MagicMock()

    r = client.post(
        "/",
        data=json.dumps(pubsub_msg),
        headers={"Content-Type": "application/json"},
    )

    shared.insert_row_into_bigquery.assert_called_with(event)
    assert r.status_code == 204
