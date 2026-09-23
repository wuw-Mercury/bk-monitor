import datetime
from typing import Any
from unittest import mock

import pytest
from django.db import connections
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from api.apm_api.default import QueryTopoNodeResource as QueryTopoNodeAPI
from apm.models import TopoNode
from apm.resources import QueryTopoNodeResource

pytestmark = [
    pytest.mark.django_db(databases="__all__"),
    pytest.mark.filterwarnings("ignore:DateTimeField TopoNode.updated_at received a naive datetime:RuntimeWarning"),
]

LEGACY_FIELDS = {"extra_data", "system", "platform", "sdk", "topo_key", "created_at", "updated_at"}


def create_node(name: str, **kwargs: Any) -> TopoNode:
    return TopoNode.objects.create(
        bk_biz_id=kwargs.pop("bk_biz_id", 2),
        app_name=kwargs.pop("app_name", "heartbeat_read"),
        topo_key=name,
        extra_data=kwargs.pop("extra_data", {"category": "other", "kind": "service"}),
        **kwargs,
    )


@pytest.mark.parametrize("flag", [None, False, "false", True, "true"])
def test_topology_protocol_opt_in(flag: bool | str | None) -> None:
    sources = {
        "legacy": [],
        "trace": ["trace"],
        "metric": ["metric"],
        "log": ["log"],
        "profiling": ["profiling"],
        "both": ["log", "profiling"],
        "reverse": ["profiling", "log"],
        "mixed": ["log", "trace"],
    }
    heartbeat = {"trace": {"last_data_at": 100, "checked_at": 110}, "log": {"last_data_at": None, "checked_at": 120}}
    for name, source in sources.items():
        create_node(name, source=source, heartbeat=heartbeat)
    params = {"bk_biz_id": 2, "app_name": "heartbeat_read"}
    if flag is not None:
        params["include_heartbeat"] = flag
    rows = QueryTopoNodeResource().request(params)
    enabled = flag in (True, "true")
    expected_names = set(sources) if enabled else {"legacy", "trace", "metric", "mixed"}
    assert {row["topo_key"] for row in rows} == expected_names
    for row in rows:
        assert set(row) == LEGACY_FIELDS | ({"source", "heartbeat"} if enabled else set())
        if enabled:
            assert row["source"] == sources[row["topo_key"]]
            assert row["heartbeat"] == heartbeat


@pytest.mark.parametrize("enabled", [False, True])
def test_topology_scope_retention_and_remote_service_filter(enabled: bool) -> None:
    create_node("visible", source=["trace"])
    create_node("other-biz", bk_biz_id=3)
    create_node("other-app", app_name="other")
    expired = create_node("expired")
    TopoNode.objects.filter(pk=expired.pk).update(
        updated_at=timezone.now() - datetime.timedelta(days=TopoNode.EXPIRED_DAYS + 1)
    )
    create_node("unsupported", extra_data={"kind": "remote_service", "category": "mysql"})
    create_node("http:remote", extra_data={"kind": "remote_service", "category": "http"})
    params = {"bk_biz_id": 2, "app_name": "heartbeat_read", "include_heartbeat": enabled}
    rows = QueryTopoNodeResource().request(params)
    assert {row["topo_key"] for row in rows} == {"visible", "http:remote"}
    assert [row["topo_key"] for row in QueryTopoNodeResource().request({**params, "topo_key": "visible"})] == [
        "visible"
    ]
    assert QueryTopoNodeResource().request({**params, "app_name": "missing"}) == []


def test_empty_heartbeat_read_does_not_write_or_query_remote_data() -> None:
    node = create_node("log", source=["log"])
    updated_at = node.updated_at
    # 心跳和来源同行读取，整个入口只需要一次节点 SELECT。
    from_db = TopoNode.objects.db
    with CaptureQueriesContext(connections[from_db]) as queries:
        rows = QueryTopoNodeResource().request(
            {"bk_biz_id": 2, "app_name": "heartbeat_read", "include_heartbeat": True, "topo_key": "log"}
        )
    assert len(queries) == 1
    assert queries[0]["sql"].lstrip().upper().startswith("SELECT")
    assert rows[0]["heartbeat"] == {}
    node.refresh_from_db()
    assert node.updated_at == updated_at
    assert node.heartbeat == {}
    assert node.source == ["log"]


@pytest.mark.parametrize("flag", ["invalid", "", None, 2])
def test_invalid_heartbeat_flag_is_rejected(flag: Any) -> None:
    serializer = QueryTopoNodeResource.RequestSerializer(
        data={"bk_biz_id": 2, "app_name": "heartbeat_read", "include_heartbeat": flag}
    )
    assert not serializer.is_valid()
    assert "include_heartbeat" in serializer.errors


def test_rpc_client_passes_opt_in_without_changing_legacy_requests() -> None:
    create_node("legacy")
    create_node("log", source=["log"])
    params = {"bk_biz_id": 2, "app_name": "heartbeat_read"}
    with (
        mock.patch("core.drf_resource.contrib.nested_api.IS_API_MODE", True),
        mock.patch.object(QueryTopoNodeAPI, "direct_request", side_effect=QueryTopoNodeResource().request) as direct,
    ):
        client = QueryTopoNodeAPI()
        legacy = client.request(params)
        extended = client.request({**params, "include_heartbeat": True})
    assert {row["topo_key"] for row in legacy} == {"legacy"}
    assert {row["topo_key"] for row in extended} == {"legacy", "log"}
    assert "include_heartbeat" not in direct.call_args_list[0].args[0]
    assert direct.call_args_list[1].args[0]["include_heartbeat"] is True
