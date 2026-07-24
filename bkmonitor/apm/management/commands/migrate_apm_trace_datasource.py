"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2025 Tencent. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import argparse
import time
from dataclasses import dataclass
from typing import Any, ClassVar

from django.core.management import BaseCommand, CommandError

from apm.core.application_config import ApplicationConfig
from apm.core.handlers.application_hepler import ApplicationHelper
from apm.models import ApmApplication, SubscriptionConfig, TraceDataSource
from apm.models.shared_datasource import SharedTraceDataSource
from apm.resources import ApplyDatasourceResource
from bkmonitor.utils.tenant import bk_biz_id_to_bk_tenant_id
from constants.apm import TelemetryDataType
from constants.common import DEFAULT_TENANT_ID
from core.drf_resource import api
from metadata.models import DataSource, DataSourceResultTable, ESStorage, ResultTable, ResultTableOption
from metadata.models.bkdata.result_table import BkBaseResultTable
from metadata.models.data_link.constants import BKBASE_NAMESPACE_BK_LOG, DataLinkResourceStatus
from metadata.models.data_link.data_link import DataLink
from metadata.models.data_link.data_link_configs import DataBusConfig, ESStorageBindingConfig, ResultTableConfig

TARGET_SHARED: str = "shared"
TARGET_EXCLUSIVE: str = "exclusive"
TARGET_CHOICES: tuple[str, str] = (TARGET_SHARED, TARGET_EXCLUSIVE)
DEFAULT_DATALINK_TIMEOUT: int = 300
DEFAULT_DATALINK_CHECK_INTERVAL: int = 10


@dataclass(frozen=True)
class DataLinkComponentState:
    kind: str
    name: str
    status: str


@dataclass(frozen=True)
class TraceDataSourceMigrationContext:
    bk_biz_id: int
    app_name: str
    application: ApmApplication
    trace_datasource: TraceDataSource | None

    @property
    def current_mode(self) -> str:
        if not self.trace_datasource:
            return "not_created"
        if self.trace_datasource.is_shared:
            return TARGET_SHARED
        return TARGET_EXCLUSIVE

    @property
    def result_table_id(self) -> str:
        if not self.trace_datasource:
            return ""
        return self.trace_datasource.result_table_id

    @property
    def has_backup(self) -> bool:
        return bool(self.trace_datasource and self.trace_datasource.backup_link_info)

    @property
    def needs_index_set_repair(self) -> bool:
        return bool(
            self.trace_datasource
            and not self.trace_datasource.is_shared
            and self.trace_datasource.result_table_id
            and not self.trace_datasource.index_set_id
        )


class Command(BaseCommand):
    help = "APM Trace 数据源迁入共享或迁出独占"

    TARGET_SHARED: ClassVar[str] = TARGET_SHARED
    TARGET_EXCLUSIVE: ClassVar[str] = TARGET_EXCLUSIVE
    TARGET_CHOICES: ClassVar[tuple[str, str]] = TARGET_CHOICES

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--target",
            choices=self.TARGET_CHOICES,
            required=True,
            help="目标模式：shared 表示迁入共享，exclusive 表示迁出独占",
        )
        parser.add_argument(
            "--apps",
            nargs="+",
            required=True,
            help="应用列表，格式为 <bk_biz_id>:<app_name>，例如 2:app_a 2:app_b",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="输出当前状态、目标状态、是否有备份和预计动作等信息，不执行迁移",
        )
        parser.add_argument(
            "--datalink-timeout",
            type=int,
            default=DEFAULT_DATALINK_TIMEOUT,
            help=f"等待 DataLink 组件就绪的超时时间（秒），默认 {DEFAULT_DATALINK_TIMEOUT}",
        )
        parser.add_argument(
            "--datalink-check-interval",
            type=int,
            default=DEFAULT_DATALINK_CHECK_INTERVAL,
            help=f"DataLink 组件状态检查间隔（秒），默认 {DEFAULT_DATALINK_CHECK_INTERVAL}",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        target: str = options["target"]
        dry_run: bool = options["dry_run"]
        datalink_timeout: int = options["datalink_timeout"]
        datalink_check_interval: int = options["datalink_check_interval"]
        if datalink_timeout < 0:
            raise CommandError("--datalink-timeout 不能小于 0")
        if datalink_check_interval <= 0:
            raise CommandError("--datalink-check-interval 必须大于 0")

        apps: list[tuple[int, str]] = self.parse_apps(options["apps"])
        contexts: list[TraceDataSourceMigrationContext] = [
            self.get_migration_context(bk_biz_id, app_name) for bk_biz_id, app_name in apps
        ]

        if dry_run:
            for index, context in enumerate(contexts):
                if index > 0:
                    self.stdout.write("")
                self.write_dry_run_status(context, target)
            return

        for context in contexts:
            self.migrate_app(
                context,
                target,
                datalink_timeout=datalink_timeout,
                datalink_check_interval=datalink_check_interval,
            )

    @staticmethod
    def parse_apps(app_values: list[str]) -> list[tuple[int, str]]:
        """解析命令行应用参数，并按首次出现顺序去重。"""
        apps: list[tuple[int, str]] = []
        seen: set[tuple[int, str]] = set()
        for app_value in app_values:
            if ":" not in app_value:
                raise CommandError(f"--apps 参数格式错误：{app_value}，期望格式为 <bk_biz_id>:<app_name>")

            bk_biz_id_text, app_name = app_value.split(":", 1)
            try:
                bk_biz_id = int(bk_biz_id_text)
            except ValueError as exc:
                raise CommandError(f"--apps 参数业务 ID 非整数：{app_value}") from exc

            if not app_name:
                raise CommandError(f"--apps 参数应用名为空：{app_value}")

            app_key = (bk_biz_id, app_name)
            if app_key in seen:
                continue
            apps.append(app_key)
            seen.add(app_key)
        return apps

    def write_dry_run_status(self, context: TraceDataSourceMigrationContext, target: str) -> None:
        """输出单个应用的 dry-run 状态。"""
        result_table_id: str = context.result_table_id
        action: str
        if not context.trace_datasource:
            action = f"create {target} datasource"
        elif context.current_mode == target and not context.needs_index_set_repair:
            action = "keep the current mode unchanged"
        elif context.current_mode == target:
            action = "repair current exclusive datasource"
        elif target == self.TARGET_SHARED:
            action = "backup exclusive link info and migrate to shared"
        elif context.has_backup:
            action = "release shared pool usage and recover exclusive link info"
        else:
            action = "release shared pool usage and create exclusive datasource"

        self.stdout.write(
            "\n".join(
                [
                    "[dry-run]",
                    f"application_id：{context.application.id}",
                    f"bk_biz_id：{context.bk_biz_id}",
                    f"app_name：{context.app_name}",
                    f"current_mode：{context.current_mode}",
                    f"target_mode：{target}",
                    f"result_table_id：{result_table_id or '-'}",
                    f"has_backup：{context.has_backup}",
                    f"action：{action}",
                ]
            )
        )

    def migrate_app(
        self,
        context: TraceDataSourceMigrationContext,
        target: str,
        datalink_timeout: int = DEFAULT_DATALINK_TIMEOUT,
        datalink_check_interval: int = DEFAULT_DATALINK_CHECK_INTERVAL,
    ) -> None:
        """迁移单个应用 Trace 数据源。"""
        if context.current_mode == target and not context.needs_index_set_repair:
            trace_datasource: TraceDataSource = self.validate_migration_result(
                context,
                target,
                datalink_timeout=datalink_timeout,
                datalink_check_interval=datalink_check_interval,
            )
            self.refresh_and_validate_collector_config(context, trace_datasource)
            self.stdout.write(
                self.style.SUCCESS(
                    f"已跳过 Trace 数据源迁移：bk_biz_id={context.bk_biz_id}, "
                    f"app_name={context.app_name}, mode={target}"
                )
            )
            return

        trace_datasource_option: dict[str, Any] = ApplicationHelper.get_default_storage_config(
            context.bk_biz_id, context.app_name
        )
        if not trace_datasource_option.get("es_storage_cluster"):
            raise CommandError(
                f"无法获取默认 Trace 存储配置：bk_biz_id={context.bk_biz_id}, app_name={context.app_name}"
            )

        shared_datasource_types: list[str] = [TelemetryDataType.TRACE.value] if target == self.TARGET_SHARED else []
        ApplyDatasourceResource().request(
            {
                "application_id": context.application.id,
                "trace_datasource_option": trace_datasource_option,
                "shared_datasource_types": shared_datasource_types,
            }
        )

        trace_datasource: TraceDataSource = self.validate_migration_result(
            context,
            target,
            datalink_timeout=datalink_timeout,
            datalink_check_interval=datalink_check_interval,
        )
        self.refresh_and_validate_collector_config(context, trace_datasource)

        self.stdout.write(
            self.style.SUCCESS(
                f"已完成 Trace 数据源迁移：bk_biz_id={context.bk_biz_id}, app_name={context.app_name}, mode={target}"
            )
        )

    def refresh_and_validate_collector_config(
        self,
        context: TraceDataSourceMigrationContext,
        trace_datasource: TraceDataSource,
    ) -> None:
        """刷新 Collector 配置，并校验可同步确认的配置状态。"""
        application_config = ApplicationConfig(context.application)
        self.validate_collector_config(context, trace_datasource, application_config.application_config)
        application_config.refresh()
        ApplicationConfig.refresh_k8s([context.application])
        self.validate_subscription_configs(context, trace_datasource)

    def validate_migration_result(
        self,
        context: TraceDataSourceMigrationContext,
        target: str,
        datalink_timeout: int,
        datalink_check_interval: int,
    ) -> TraceDataSource:
        """校验迁移结果中可同步确认的关键状态。"""
        trace_datasource: TraceDataSource | None = TraceDataSource.objects.filter(
            bk_biz_id=context.bk_biz_id,
            app_name=context.app_name,
        ).first()
        if not trace_datasource:
            raise self.build_validation_error(context, ["TraceDataSource 不存在"])

        errors: list[str] = []
        expected_shared: bool = target == self.TARGET_SHARED
        if trace_datasource.is_shared != expected_shared:
            errors.append(f"目标模式应为 {target}，实际为 {'shared' if trace_datasource.is_shared else 'exclusive'}")
        if not trace_datasource.is_ready():
            errors.append(
                f"DataID/RT 未就绪：bk_data_id={trace_datasource.bk_data_id}, "
                f"result_table_id={trace_datasource.result_table_id or '-'}"
            )

        bk_tenant_id: str = (
            DEFAULT_TENANT_ID if trace_datasource.is_shared else bk_biz_id_to_bk_tenant_id(context.bk_biz_id)
        )
        result_table: ResultTable | None = ResultTable.objects.filter(
            bk_tenant_id=bk_tenant_id,
            table_id=trace_datasource.result_table_id,
        ).first()
        if not result_table:
            errors.append(f"目标 ResultTable 不存在：{trace_datasource.result_table_id}")
        else:
            expected_enabled: bool = trace_datasource.is_shared or context.application.is_enabled_trace
            if result_table.is_enable != expected_enabled:
                errors.append(f"ResultTable.is_enable 应为 {expected_enabled}，实际为 {result_table.is_enable}")

        if not DataSource.objects.filter(
            bk_tenant_id=bk_tenant_id,
            bk_data_id=trace_datasource.bk_data_id,
        ).exists():
            errors.append(f"目标 DataSource 不存在：bk_data_id={trace_datasource.bk_data_id}")
        if not DataSourceResultTable.objects.filter(
            bk_tenant_id=bk_tenant_id,
            bk_data_id=trace_datasource.bk_data_id,
            table_id=trace_datasource.result_table_id,
        ).exists():
            errors.append(
                f"DataID 与 RT 关系不存在：bk_data_id={trace_datasource.bk_data_id}, "
                f"result_table_id={trace_datasource.result_table_id}"
            )

        if trace_datasource.is_shared:
            shared_datasource: SharedTraceDataSource | None = SharedTraceDataSource.objects.filter(
                pk=trace_datasource.shared_datasource_id
            ).first()
            if not shared_datasource:
                errors.append(f"共享数据源不存在：shared_datasource_id={trace_datasource.shared_datasource_id}")
            elif not shared_datasource.is_enabled:
                errors.append(f"共享数据源未启用：shared_datasource_id={shared_datasource.id}")
            elif not 0 <= shared_datasource.usage_count <= shared_datasource.quota:
                errors.append(
                    f"共享数据源用量异常：usage_count={shared_datasource.usage_count}, quota={shared_datasource.quota}"
                )
            else:
                enabled_reference_count: int = self.get_enabled_shared_reference_count(shared_datasource.id)
                if shared_datasource.usage_count != enabled_reference_count:
                    errors.append(
                        f"共享数据源用量与启用应用引用数不一致：usage_count={shared_datasource.usage_count}, "
                        f"enabled_reference_count={enabled_reference_count}"
                    )

        should_validate_active_link: bool = trace_datasource.is_shared or context.application.is_enabled_trace
        if should_validate_active_link:
            self.validate_index_set(trace_datasource, errors)
            if trace_datasource.is_bkbase_v4_link():
                self.validate_v4_local_config(trace_datasource, bk_tenant_id, errors)

        if errors:
            raise self.build_validation_error(context, errors)

        if should_validate_active_link and trace_datasource.is_bkbase_v4_link():
            self.wait_for_datalink_ready(
                context,
                trace_datasource,
                bk_tenant_id=bk_tenant_id,
                timeout=datalink_timeout,
                check_interval=datalink_check_interval,
            )
        return trace_datasource

    @staticmethod
    def get_enabled_shared_reference_count(shared_datasource_id: int) -> int:
        """统计实际启用且引用指定共享数据源的 Trace 应用数。"""
        datasource_app_keys: set[tuple[int, str]] = set(
            TraceDataSource.objects.filter(shared_datasource_id=shared_datasource_id).values_list(
                "bk_biz_id",
                "app_name",
            )
        )
        if not datasource_app_keys:
            return 0

        bk_biz_ids: set[int] = {bk_biz_id for bk_biz_id, _ in datasource_app_keys}
        app_names: set[str] = {app_name for _, app_name in datasource_app_keys}
        enabled_app_keys: set[tuple[int, str]] = set(
            ApmApplication.objects.filter(
                bk_biz_id__in=bk_biz_ids,
                app_name__in=app_names,
                is_enabled_trace=True,
            ).values_list(
                "bk_biz_id",
                "app_name",
            )
        )
        return len(datasource_app_keys & enabled_app_keys)

    @staticmethod
    def validate_index_set(
        trace_datasource: TraceDataSource,
        errors: list[str],
    ) -> None:
        """校验 Trace 查询依赖的日志平台索引集。"""
        if not trace_datasource.index_set_id:
            errors.append("Trace 索引集 ID 为空")
            return

        try:
            index_set: dict[str, Any] = api.log_search.log_search_index_set(index_set_id=trace_datasource.index_set_id)
        except Exception as exc:
            errors.append(f"Trace 索引集查询失败：index_set_id={trace_datasource.index_set_id}, error={exc}")
            return
        if not index_set:
            errors.append(f"Trace 索引集不存在：index_set_id={trace_datasource.index_set_id}")

    @staticmethod
    def validate_v4_local_config(
        trace_datasource: TraceDataSource,
        bk_tenant_id: str,
        errors: list[str],
    ) -> None:
        """校验 V4 DataLink 下发所依赖的本地配置。"""
        enabled_option: ResultTableOption | None = ResultTableOption.objects.filter(
            bk_tenant_id=bk_tenant_id,
            table_id=trace_datasource.result_table_id,
            name=ResultTableOption.OPTION_ENABLE_V4_LOG_DATA_LINK,
        ).first()
        if not enabled_option or not enabled_option.get_value():
            errors.append("Trace ResultTable 未启用 V4 DataLink")

        datalink_option: ResultTableOption | None = ResultTableOption.objects.filter(
            bk_tenant_id=bk_tenant_id,
            table_id=trace_datasource.result_table_id,
            name=ResultTableOption.OPTION_V4_LOG_DATA_LINK,
        ).first()
        if not datalink_option:
            errors.append("Trace ResultTable 缺少 V4 DataLink 配置")
        if not ESStorage.objects.filter(
            bk_tenant_id=bk_tenant_id,
            table_id=trace_datasource.result_table_id,
        ).exists():
            errors.append(f"Trace ResultTable 缺少 ESStorage：{trace_datasource.result_table_id}")

    def wait_for_datalink_ready(
        self,
        context: TraceDataSourceMigrationContext,
        trace_datasource: TraceDataSource,
        bk_tenant_id: str,
        timeout: int,
        check_interval: int,
    ) -> None:
        """等待目标 DataLink 的 ResultTable、ES Binding 和 Databus 全部就绪。"""
        bkbase_result_tables: list[BkBaseResultTable] = list(
            BkBaseResultTable.objects.filter(
                bk_tenant_id=bk_tenant_id,
                monitor_table_id=trace_datasource.result_table_id,
            )
        )
        if not bkbase_result_tables:
            raise self.build_validation_error(
                context,
                [f"目标 RT 缺少 BkBaseResultTable：{trace_datasource.result_table_id}"],
            )

        datalink_names: list[str] = [record.data_link_name for record in bkbase_result_tables]
        datalink: DataLink | None = (
            DataLink.objects.filter(
                bk_tenant_id=bk_tenant_id,
                data_link_name__in=datalink_names,
                data_link_strategy=DataLink.BK_LOG,
                namespace=BKBASE_NAMESPACE_BK_LOG,
            )
            .order_by("-last_modify_time")
            .first()
        )
        if not datalink:
            raise self.build_validation_error(
                context,
                [f"目标 BK_LOG DataLink 不存在：candidates={datalink_names}"],
            )

        errors: list[str] = []
        if datalink.bk_data_id != trace_datasource.bk_data_id:
            errors.append(f"DataLink.bk_data_id={datalink.bk_data_id}，目标值为 {trace_datasource.bk_data_id}")
        if trace_datasource.result_table_id not in datalink.table_ids:
            errors.append(f"DataLink.table_ids 未包含目标 RT：{trace_datasource.result_table_id}")
        if errors:
            raise self.build_validation_error(context, errors)

        self.stdout.write(
            f"等待 Trace DataLink 就绪：bk_biz_id={context.bk_biz_id}, app_name={context.app_name}, "
            f"data_link_name={datalink.data_link_name}"
        )
        deadline: float = time.monotonic() + timeout
        states: list[DataLinkComponentState] = []
        while True:
            states = self.get_datalink_component_states(
                context,
                bk_tenant_id,
                datalink.data_link_name,
                datalink.namespace,
            )
            if all(state.status == DataLinkResourceStatus.OK.value for state in states):
                return

            failed_states: list[DataLinkComponentState] = [
                state for state in states if state.status == DataLinkResourceStatus.FAILED.value
            ]
            if failed_states:
                raise self.build_validation_error(
                    context,
                    [f"DataLink 组件失败：{self.format_component_states(failed_states)}"],
                )

            remaining: float = deadline - time.monotonic()
            if remaining <= 0:
                raise self.build_validation_error(
                    context,
                    [f"等待 DataLink 组件就绪超时（{timeout} 秒）：{self.format_component_states(states)}"],
                )
            time.sleep(min(check_interval, remaining))

    def get_datalink_component_states(
        self,
        context: TraceDataSourceMigrationContext,
        bk_tenant_id: str,
        data_link_name: str,
        namespace: str,
    ) -> list[DataLinkComponentState]:
        """读取 Trace DataLink 三个必要组件的 BKBase 实时状态。"""
        states: list[DataLinkComponentState] = []
        for component_model in (ResultTableConfig, ESStorageBindingConfig, DataBusConfig):
            components: list[Any] = list(
                component_model.objects.filter(
                    bk_tenant_id=bk_tenant_id,
                    data_link_name=data_link_name,
                    namespace=namespace,
                )
            )
            if not components:
                raise self.build_validation_error(
                    context,
                    [f"DataLink 本地组件不存在：kind={component_model.kind}, data_link_name={data_link_name}"],
                )

            for component in components:
                component_config: dict[str, Any] | None = component.component_config
                status: str = component_config.get("status", {}).get("phase") if component_config else "Absent/Unknown"
                states.append(
                    DataLinkComponentState(
                        kind=component.kind,
                        name=component.name,
                        status=status or "Unknown",
                    )
                )
        return states

    @staticmethod
    def validate_collector_config(
        context: TraceDataSourceMigrationContext,
        trace_datasource: TraceDataSource,
        application_config: dict[str, Any],
    ) -> None:
        """确认本次要下发的 Collector 配置已切换到目标 Trace DataID。"""
        expected_trace_data_id: int | None = (
            trace_datasource.bk_data_id if context.application.is_enabled_trace else None
        )
        actual_trace_data_id: int | None = application_config.get("trace_data_id")
        if actual_trace_data_id != expected_trace_data_id:
            raise Command.build_validation_error(
                context,
                [f"Collector 配置 trace_data_id={actual_trace_data_id}，目标值为 {expected_trace_data_id}"],
            )

    @staticmethod
    def validate_subscription_configs(
        context: TraceDataSourceMigrationContext,
        trace_datasource: TraceDataSource,
    ) -> None:
        """已有节点管理订阅必须已刷新到目标 Trace DataID。"""
        expected_trace_data_id: int | None = (
            trace_datasource.bk_data_id if context.application.is_enabled_trace else None
        )
        stale_subscription_ids: list[int] = []
        subscriptions: list[SubscriptionConfig] = list(
            SubscriptionConfig.objects.filter(
                bk_biz_id=context.bk_biz_id,
                app_name=context.app_name,
            )
        )
        for subscription in subscriptions:
            steps: list[dict[str, Any]] = subscription.config.get("steps") or []
            application_config: dict[str, Any] = steps[0].get("params", {}).get("context", {}) if steps else {}
            if application_config.get("trace_data_id") != expected_trace_data_id:
                stale_subscription_ids.append(subscription.subscription_id)

        if stale_subscription_ids:
            raise Command.build_validation_error(
                context,
                [f"节点管理订阅未刷新到目标 Trace DataID：subscription_ids={stale_subscription_ids}"],
            )

    @staticmethod
    def format_component_states(states: list[DataLinkComponentState]) -> str:
        return ", ".join(f"{state.kind}/{state.name}={state.status}" for state in states)

    @staticmethod
    def build_validation_error(
        context: TraceDataSourceMigrationContext,
        errors: list[str],
    ) -> CommandError:
        details: str = "；".join(errors)
        return CommandError(
            f"Trace 数据源迁移结果校验失败：bk_biz_id={context.bk_biz_id}, app_name={context.app_name}；{details}"
        )

    @staticmethod
    def get_migration_context(bk_biz_id: int, app_name: str) -> TraceDataSourceMigrationContext:
        application: ApmApplication | None = ApmApplication.objects.filter(
            bk_biz_id=bk_biz_id, app_name=app_name
        ).first()
        if not application:
            raise CommandError(f"业务下应用不存在：bk_biz_id={bk_biz_id}, app_name={app_name}")

        trace_datasource: TraceDataSource | None = TraceDataSource.objects.filter(
            bk_biz_id=bk_biz_id, app_name=app_name
        ).first()
        return TraceDataSourceMigrationContext(
            bk_biz_id=bk_biz_id,
            app_name=app_name,
            application=application,
            trace_datasource=trace_datasource,
        )
