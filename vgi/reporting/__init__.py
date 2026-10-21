# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""Proposed reporting protocol definitions, with no service implementations.

Dataclasses are the schema source of truth. The interfaces are independently
hostable once implemented; importing them does not register endpoints or SQL.
"""

from types import ModuleType
from typing import Any

from vgi.reporting import (
    alerts,
    common,
    delegations,
    discovery,
    notify,
    objects,
    ownership,
    render,
    reports,
    resource_identity,
    resource_ownership,
    schedules,
    schedules_v2,
    sources,
    sql_tasks,
)
from vgi.reporting.alerts import AlertsProtocol as AlertsProtocol
from vgi.reporting.delegations import DelegationsProtocol as DelegationsProtocol
from vgi.reporting.notify import NotifyProtocol as NotifyProtocol
from vgi.reporting.objects import ObjectsProtocol as ObjectsProtocol
from vgi.reporting.ownership import ReportOwnershipProtocol as ReportOwnershipProtocol
from vgi.reporting.render import ReportRenderProtocol as ReportRenderProtocol
from vgi.reporting.reports import ReportsProtocol as ReportsProtocol
from vgi.reporting.resource_identity import ResourceIdentityProtocol as ResourceIdentityProtocol
from vgi.reporting.resource_ownership import OwnershipProtocol as OwnershipProtocol
from vgi.reporting.schedules import SchedulesProtocol as SchedulesProtocol
from vgi.reporting.schedules_v2 import SchedulesProtocolV2 as SchedulesProtocolV2
from vgi.reporting.sql_tasks import SqlTasksProtocol as SqlTasksProtocol

CONTRACT_MODULES: tuple[ModuleType, ...] = (
    common,
    discovery,
    resource_identity,
    sources,
    objects,
    resource_ownership,
    schedules_v2,
    reports,
    ownership,
    render,
    schedules,
    sql_tasks,
    alerts,
    notify,
    delegations,
)
PROTOCOLS: tuple[type[Any], ...] = (
    ReportsProtocol,
    ObjectsProtocol,
    OwnershipProtocol,
    ResourceIdentityProtocol,
    SchedulesProtocolV2,
    ReportOwnershipProtocol,
    ReportRenderProtocol,
    SchedulesProtocol,
    SqlTasksProtocol,
    AlertsProtocol,
    NotifyProtocol,
    DelegationsProtocol,
)
