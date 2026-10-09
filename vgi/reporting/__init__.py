# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed reporting protocol definitions, with no service implementations.

Dataclasses are the schema source of truth. The seven interfaces are independently
hostable once implemented; importing them does not register endpoints or SQL.
"""

from types import ModuleType
from typing import Any

from vgi.reporting import alerts, common, delegations, notify, render, reports, schedules, sql_tasks
from vgi.reporting.alerts import AlertsProtocol as AlertsProtocol
from vgi.reporting.delegations import DelegationsProtocol as DelegationsProtocol
from vgi.reporting.notify import NotifyProtocol as NotifyProtocol
from vgi.reporting.render import ReportRenderProtocol as ReportRenderProtocol
from vgi.reporting.reports import ReportsProtocol as ReportsProtocol
from vgi.reporting.schedules import SchedulesProtocol as SchedulesProtocol
from vgi.reporting.sql_tasks import SqlTasksProtocol as SqlTasksProtocol

CONTRACT_MODULES: tuple[ModuleType, ...] = (common, reports, render, schedules, sql_tasks, alerts, notify, delegations)
PROTOCOLS: tuple[type[Any], ...] = (
    ReportsProtocol,
    ReportRenderProtocol,
    SchedulesProtocol,
    SqlTasksProtocol,
    AlertsProtocol,
    NotifyProtocol,
    DelegationsProtocol,
)
