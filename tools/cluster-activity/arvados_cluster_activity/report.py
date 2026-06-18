#!/usr/bin/env python3
# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

import collections
import csv
import dataclasses
import datetime
import enum
import functools
import itertools
import json
import logging
import re
import statistics
import urllib.parse

from typing import Callable, ClassVar, Dict, List, Mapping

import arvados.api_resources as arv_types
import arvados.util
import ciso8601

from arvados_cluster_activity.prometheus import get_metric_usage, get_data_usage
from arvados_cluster_activity.reportchart import ReportChart

NOT_AVAILABLE = "(Not Available)"

@dataclasses.dataclass
class BytesFormatter:
    """Function to format a number of bytes as a human-friendly string"""
    exp: int
    suffixes: list[str]

    def __call__(self, bytecount):
        if bytecount is None:
            return None
        for suffix in self.suffixes:
            bytecount /= self.exp
            if bytecount < self.exp:
                break
        return f'{bytecount:.3f} {suffix}'


bytes_base2_fmt = BytesFormatter(1024, ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB'])
format_with_suffix_base2 = bytes_base2_fmt
bytes_si_fmt = BytesFormatter(1000, ['KB', 'MB', 'GB', 'TB', 'PB', 'EB'])
format_with_suffix_base10 = bytes_si_fmt

def cost_fmt(cost):
    return f'${cost:,.02f}'


def datespan_fmt(begin, end):
    begin_isodate = begin.strftime('%Y-%m-%d')
    end_isodate = end.strftime('%Y-%m-%d')
    if begin_isodate == end_isodate:
        return begin_isodate
    else:
        return f'{begin_isodate} to {end_isodate}'


def datetime_fmt(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def duration_hms_fmt(seconds):
    if isinstance(seconds, datetime.timedelta):
        seconds = seconds.total_seconds()
    mins, secs = divmod(round(seconds), 60)
    hrs, mins = divmod(mins, 60)
    return f'{hrs}:{mins:02d}:{secs:02d}'


def duration_hrs_fmt(seconds, suffix=''):
    if isinstance(seconds, datetime.timedelta):
        seconds = seconds.total_seconds()
    sep = ' ' if suffix and not suffix.startswith(' ') else ''
    return f'{seconds / 3600:,.1f}{sep}{suffix}'


class S3CostPeriod(enum.IntEnum):
    MONTHLY = 1
    DAILY = 30
    HOURLY = 30 * 24


def s3_cost(num_bytes, period):
    """Calculate the USD cost of storing `num_bytes` over the time `period`"""
    gb = num_bytes / (1 << 30)
    first_50tb = min(1024*50, gb)
    next_450tb = max(min(1024*450, gb-1024*50), 0)
    over_500tb = max(gb-1024*500, 0)
    monthly_cost = (first_50tb * 0.023) + (next_450tb * 0.022) + (over_500tb * 0.021)
    return monthly_cost / period.value


def aws_monthly_cost(value):
    return s3_cost(value, S3CostPeriod.MONTHLY)


@dataclasses.dataclass(slots=True)
class WorkflowRun:
    """Encapsulate and query a run container

    This class bundles together a satisfied container request, its container,
    and other associated records like owner, modifying user, and workflow.
    Methods fetch data from the low-level records with appropriate fallbacks.

    It's called `WorkflowRun` because the report is focused on workflows and
    defaults reflect that, but in principle it can wrap any container request
    plus container.
    """
    request: arv_types.ContainerRequest
    container: arv_types.Container
    owner: arv_types.Group | arv_types.User | None = None
    user: arv_types.User | None = None
    workflow: arv_types.Workflow | None = None

    def container_cost(self):
        return self.container['cost']

    def created_at(self):
        return ciso8601.parse_datetime(self.request['created_at'])

    def cumulative_cost(self):
        return self.request['cumulative_cost']

    def finished_at(self):
        return ciso8601.parse_datetime(self.container['finished_at'])

    def name_key(self):
        if self.workflow is None:
            return self.request_name_without_suffix()
        else:
            return self.workflow['name']

    def owner_name(self):
        if self.owner is None:
            return 'unknown owner'
        try:
            return self.owner['full_name']
        except KeyError:
            return self.owner['name']

    def request_name_without_suffix(self):
        """Return the request name without any numbered suffix

        This provides a normalized name for workflow steps with fanout.
        """
        return re.sub(r'_[0-9]+$', '', self.request['name'])

    def runtime(self):
        return self.finished_at() - self.started_at()

    def started_at(self):
        return ciso8601.parse_datetime(self.container['started_at'])

    def start_end_runtime(self):
        """Return start time, finish time, and runtime delta in one call

        Most use cases want all three and this prevents needless re-parsing."""
        start = self.started_at()
        finish = self.finished_at()
        return start, finish, finish - start

    def step_name(self):
        if self.request['requesting_container_uuid'] is None:
            return 'workflow runner'
        else:
            return self.request_name_without_suffix()

    def template_uuid(self, default=None):
        try:
            return self.request['properties']['template_uuid']
        except KeyError:
            return default

    def user_name(self):
        if self.user is None:
            return 'unknown user'
        else:
            return self.user['full_name']

    def workflow_name(self, default=None):
        if self.workflow is not None:
            return self.workflow['name']
        else:
            return self.template_uuid(default)

    def csv_row(self):
        started, finished, runtime = self.start_end_runtime()
        return {
            'Project': self.owner_name(),
            'ProjectUUID': self.request['owner_uuid'],
            'Workflow': self.workflow_name('workflow run from command line'),
            'WorkflowUUID': self.template_uuid('none'),
            'Step': self.step_name(),
            'StepUUID': self.request['uuid'],
            'Sample': self.request['name'],
            'SampleUUID': self.request['uuid'],
            'User': self.user_name(),
            'UserUUID': self.request['modified_by_user_uuid'],
            'Submitted': datetime_fmt(self.created_at()),
            'Started': datetime_fmt(started),
            'Finished': datetime_fmt(finished),
            'Runtime': duration_hms_fmt(runtime),
            'Cost': self.container_cost(),
            'CumulativeCost': self.cumulative_cost(),
        }


@dataclasses.dataclass(slots=True)
class WorkflowRunSummary:
    """Collect summary statistics about a series of related WorkflowRuns

    Call the `tally()` method with a series of WorkflowRuns, then call the
    other methods to report collected statistics. The caller determines how
    the WorkflowRuns are related; nothing in this class enforces any relation.
    Refer to the `summaries` field of `ClusterActivityReport`.

    This class collects basic high-level statistics about related runs. Most
    fields are a single cumulative data point. The only exception is `users`
    which will grow much slower than the number of runs in most real-world
    applications.
    """
    owner_uuid: str | None = None

    cost: float = dataclasses.field(init=False, default=0.0)
    count: int = dataclasses.field(init=False, default=0)
    earliest: datetime.datetime = dataclasses.field(
        init=False,
        default=datetime.datetime.max.replace(tzinfo=datetime.timezone.utc),
    )
    latest: datetime.datetime = dataclasses.field(
        init=False,
        default=datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
    )
    runtime: datetime.timedelta = dataclasses.field(init=False, default_factory=datetime.timedelta)
    users: set[str] = dataclasses.field(init=False, default_factory=set)

    def tally(self, run):
        started, finished, runtime = run.start_end_runtime()
        self.earliest = min(self.earliest, started)
        self.latest = max(self.latest, finished)
        self.cost += run.cumulative_cost()
        self.count += 1
        self.runtime += runtime
        self.users.add(run.user['uuid'])

    def total_cost(self):
        return self.cost

    def total_runtime(self):
        return self.runtime.total_seconds()

    def total_runs(self):
        return self.count

    def total_users(self):
        return len(self.users)

    def usernames(self, users_map, default_name='unknown user'):
        default_user = {'full_name': default_name}
        return sorted(
            users_map.get(uuid, default_user)['full_name']
            for uuid in self.users
        )


@dataclasses.dataclass(slots=True)
class WorkflowRunStatistics:
    """Collect detailed statistics about a series of related WorkflowRuns

    Call the `tally()` method with a series of WorkflowRuns, then call the
    other methods to report collected statistics. The caller determines how
    the WorkflowRuns are related; nothing in this class enforces any relation.
    Refer to the `statistics` field of `ClusterActivityReport`.

    This class collects a few data points per tallied run, so you can expect
    it to grow in RAM use O(n) with the number of runs.
    """
    owner_uuid: str | None = None
    workflow_uuid: str | None = None

    costs: list[float] = dataclasses.field(init=False, default_factory=list)
    runtimes: list[float] = dataclasses.field(init=False, default_factory=list)

    def tally(self, run):
        self.costs.append(run.cumulative_cost())
        self.runtimes.append(run.runtime().total_seconds())

    def mean_cost(self):
        return statistics.mean(self.costs)

    def median_cost(self):
        return statistics.median(self.costs)

    def total_cost(self):
        return sum(self.costs)

    def mean_runtime(self):
        return statistics.mean(self.runtimes)

    def median_runtime(self):
        return statistics.median(self.runtimes)

    def total_runtime(self):
        return sum(self.runtimes)

    def total_runs(self):
        return len(self.costs)


@dataclasses.dataclass
class ProgressLogger:
    """Log progress over subsequences of a larger sequence"""
    log_fmt: str
    logger: logging.Logger = dataclasses.field(default_factory=logging.getLogger)
    loglevel: int = dataclasses.field(default=logging.INFO)
    count: int = dataclasses.field(init=False, default=0)

    def report(self, seq):
        start = self.count + 1
        self.count += len(seq)
        self.logger.log(self.loglevel, self.log_fmt, start, self.count)


def key_by_uuid(items):
    return ((item['uuid'], item) for item in items)


@dataclasses.dataclass
class ClusterActivityReport:
    since: datetime.datetime
    to: datetime.datetime
    arv_client: arv_types.ArvadosAPIClient = dataclasses.field(default_factory=arvados.api)
    prom_client: 'prometheus_api_client.PrometheusConnect | None' = None
    exclude: dataclasses.InitVar[str] = ''

    cluster: str = dataclasses.field(init=False)
    should_exclude: Callable[[str], bool] = dataclasses.field(init=False)
    # A WorkflowRunSummary for each project. The None key summarizes all runs.
    summaries: dict[str | None, WorkflowRunSummary] = dataclasses.field(init=False, default_factory=dict)
    # Statistics for each workflow. The outer key is an owner name. The inner
    # key is a workflow UUID.
    statistics: dict[str, dict[str, WorkflowRunStatistics]] = dataclasses.field(
        init=False,
        default_factory=lambda: collections.defaultdict(dict),
    )
    # Map container request UUIDs to workflow UUIDs
    workflow_map: dict[str, str] = dataclasses.field(init=False, default_factory=dict)
    # Map each type of object by UUID to the loaded API object
    groups: dict[str, arv_types.Group] = dataclasses.field(init=False, default_factory=dict)
    users: dict[str, arv_types.User] = dataclasses.field(init=False, default_factory=dict)
    workflows: dict[str, arv_types.Workflow] = dataclasses.field(init=False, default_factory=dict)
    # A chainmap of groups+users
    owners: Mapping[str, arv_types.Group | arv_types.User] = dataclasses.field(init=False)
    # UUIDs of containers that have been loaded. Used to find steps for csv_report.
    ctr_uuids: set[str] = dataclasses.field(init=False, default_factory=set)

    _request_select: ClassVar[list[str]] = [
        'container_uuid',
        'created_at',
        'cumulative_cost',
        'modified_by_user_uuid',
        'name',
        'owner_uuid',
        'requesting_container_uuid',
        'uuid',
    ]

    def __post_init__(self, exclude):
        self.cluster = self.arv_client.config()["ClusterID"]
        self.owners = collections.ChainMap(self.groups, self.users)
        if exclude:
            self.should_exclude = re.compile(exclude, re.IGNORECASE).search
        else:
            self.should_exclude = lambda s: False

    def _since_filter(self):
        return ['created_at', '>=', self.since.strftime('%Y-%m-%dT00:00Z')]

    def _to_filter(self):
        return ['created_at', '<', self.to.strftime('%Y-%m-%dT00:00Z')]

    def _load_workflows(self, requests):
        new_map = {
            req['uuid']: wf_uuid
            for req in requests
            if (wf_uuid := req['properties'].get('template_uuid'))
        }
        self.workflow_map.update(new_map)
        if new_wfs := frozenset(new_map.values()).difference(self.workflows):
            self.workflows.update(key_by_uuid(arvados.util.keyset_list_all(
                self.arv_client.workflows().list,
                filters=[['uuid', 'in', list(new_wfs)]],
                select=['uuid', 'name'],
            )))

    def _load_groups_and_users(self, requests):
        user_uuids = {req['modified_by_user_uuid'] for req in requests}
        group_uuids = set()
        for req in requests:
            owner_uuid = req['owner_uuid']
            if arvados.util.user_uuid_pattern.fullmatch(owner_uuid):
                user_uuids.add(owner_uuid)
            elif arvados.util.group_uuid_pattern.fullmatch(owner_uuid):
                group_uuids.add(owner_uuid)

        if new_users := user_uuids.difference(self.users):
            self.users.update(key_by_uuid(arvados.util.keyset_list_all(
                self.arv_client.users().list,
                filters=[['uuid', 'in', list(new_users)]],
                select=['uuid', 'full_name', 'first_name', 'last_name'],
            )))
        if new_groups := group_uuids.difference(self.groups):
            self.groups.update(key_by_uuid(arvados.util.keyset_list_all(
                self.arv_client.groups().list,
                filters=[['uuid', 'in', list(new_groups)]],
                select=['uuid', 'name'],
            )))

    def _load_and_iter_runs(self, requests, containers, logger):
        """Iterate WorkflowRuns for the given sequence of requests

        `requests` is a sequence of Arvados container request records. This
        method yields a corresponding WorkflowRun for each, loading other
        resources from Arvados as needed to build them.
        """
        if not requests:
            return
        logger.report(requests)
        self._load_groups_and_users(requests)
        if new_cuuids := frozenset(req['container_uuid'] for req in requests).difference(containers):
            containers.update(key_by_uuid(arvados.util.keyset_list_all(
                self.arv_client.containers().list,
                filters=[['uuid', 'in', list(new_cuuids)]],
                select=['uuid', 'started_at', 'finished_at', 'cost'],
            )))
        for req in requests:
            container = containers[req['container_uuid']]
            # If the container doesn't have `finished_at` set, then it's some
            # aborted attempt to run, and there's not enough info to report it.
            if container['finished_at']:
                yield WorkflowRun(
                    req,
                    container,
                    owner=self.owners.get(req['owner_uuid']),
                    user=self.users.get(req['modified_by_user_uuid']),
                    workflow=self.workflows.get(self.workflow_map.get(req['uuid'])),
                )

    def _iter_runs(self):
        """Iterate all WorkflowRuns for this report"""
        containers = {}
        logger = ProgressLogger("Exporting workflow runs %s - %s")
        wf_reqs = arvados.util.keyset_list_all(
            self.arv_client.container_requests().list,
            filters=[
                ['command', 'like', '["arvados-cwl-runner"%'],
                ['container_uuid', '!=', None],
                self._since_filter(),
                self._to_filter(),
            ],
            select=self._request_select + ['properties'],
        )
        # Arvados will never return more than 1000 results to a single list
        # query. We go through requests in batches of 999 to maximize the
        # chances that queries for associated resources will fit in a single
        # page of results.
        while new_reqs := list(itertools.islice(wf_reqs, 999)):
            self._load_workflows(new_reqs)
            yield from self._load_and_iter_runs(new_reqs, containers, logger)

    def iter_and_tally_runs(self):
        """Iterate, filter, and build summaries for top-level WorkflowRuns"""
        if self.owners:
            return
        self.summaries[None] = WorkflowRunSummary(None)
        for run in self._iter_runs():
            run_key = run.name_key()
            if self.should_exclude(run_key):
                continue
            yield run
            self.ctr_uuids.add(run.request['container_uuid'])
            self.summaries[None].tally(run)

            owner_uuid = run.request['owner_uuid']
            owner_name = run.owner_name()
            try:
                summary = self.summaries[owner_name]
            except KeyError:
                summary = self.summaries[owner_name] = WorkflowRunSummary(owner_uuid)
            summary.tally(run)

            wf_uuid = run.template_uuid()
            try:
                stats = self.statistics[owner_name][wf_uuid]
            except KeyError:
                stats = self.statistics[owner_name][wf_uuid] = WorkflowRunStatistics(owner_uuid, wf_uuid)
            stats.tally(run)

    def iter_steps(self):
        logging.info("Getting workflow steps")
        containers = {}
        logger = ProgressLogger("Got workflow steps %s - %s")
        seen_cuuids = set()
        while new_cuuids := self.ctr_uuids.difference(seen_cuuids):
            # The ideal slice length here really depends on how many steps
            # the typical workflow starts. 50 is just a fuzzy attempt to
            # balance batching queries while keeping result sizes reasonable.
            # It could be adjusted if better data suggests that's a good idea.
            cuuid_batch = list(itertools.islice(new_cuuids, 50))
            new_reqs = list(arvados.util.keyset_list_all(
                self.arv_client.container_requests().list,
                filters=[
                    ['requesting_container_uuid', 'in', cuuid_batch],
                    ['container_uuid', '!=', None],
                ],
                select=self._request_select,
            ))
            self.workflow_map.update(
                (req['uuid'], wf_uuid)
                for req in new_reqs
                if (wf_uuid := self.workflow_map.get(req['requesting_container_uuid']))
            )
            yield from self._load_and_iter_runs(new_reqs, containers, logger)
            seen_cuuids.update(cuuid_batch)

    @functools.cached_property
    def total_users(self):
        return self.arv_client.users().list(
            filters=[
                ['is_active', '=', True],
            ],
            limit=0,
            count='exact',
        ).execute()['items_available']

    @functools.cached_property
    def total_projects(self):
        return self.arv_client.groups().list(
            filters=[
                ['group_class', '=', 'project'],
            ],
            limit=0,
            count='exact',
        ).execute()['items_available']

    def collect_graph(self, since, to, metric, resample_to):
        return [
            list(t[:2])
            for series in get_metric_usage(
                    self.prom_client,
                    since,
                    to,
                    f"{metric}{{cluster='{self.cluster}'}}",
                    resampleTo=resample_to,
            )
            for t in series.itertuples()
        ]

    def html_report(self):
        """Get a cluster activity report for the desired time period,
        returning a string containing the report as an HTML document."""
        for _ in self.iter_and_tally_runs():
            # HTML doesn't report individual runs, just summaries.
            pass

        logging.info("Getting container hours time series")
        containers_graph = self.collect_graph(
            self.since,
            self.to,
            "arvados_dispatchcloud_containers_running",
            resample_to="5min",
        )
        logging.info("Getting data usage time series")
        managed_graph = self.collect_graph(
            self.since,
            self.to,
            "arvados_keep_collection_bytes",
            resample_to="60min",
        )
        storage_graph = self.collect_graph(
            self.since,
            self.to,
            "arvados_keep_total_bytes",
            resample_to="60min",
        )

        wb_url = self.arv_client.config()["Services"]["Workbench2"]["ExternalURL"]
        def wb_link(path, text):
            return '<a href="{}">{}</a>'.format(
                urllib.parse.urljoin(wb_url, path),
                text,
            )

        cards = []
        data_rows = []
        def add_row(key, val):
            match val:
                case None:
                    val = NOT_AVAILABLE
                case int(_):
                    val = f'{val:,d}'
                case float(_):
                    val = f'{val:,.2f}'
            data_rows.append(f"<tr><th>{key}</th><td>{val}</td></tr>")
        def add_card(header, suffix):
            table_data = '\n'.join(data_rows)
            cards.append(f"""<h2>{header}</h2>
            <table class='aggtable'><tbody>
            {table_data}
            </tbody></table>
            <p>{suffix}</p>
            """)
            data_rows.clear()

        try:
            _, managed_data_end = managed_graph[-1]
        except IndexError:
            managed_data_end = None
        if storage_graph:
            _, storage_used_end = storage_graph[-1]
            storage_cost_end = s3_cost(storage_used_end, S3CostPeriod.MONTHLY)
        else:
            storage_used_end = None
            storage_cost_end = NOT_AVAILABLE
        if managed_data_end and storage_used_end:
            dedup_ratio = managed_data_end / storage_used_end
        else:
            dedup_ratio = NOT_AVAILABLE

        if self.to.date() == datetime.date.today():
            # The deduplication ratio overstates things a bit, you can
            # have collections which reference a small slice of a large
            # block, and this messes up the intuitive value of this ratio
            # and exagerates the effect.
            #
            # So for now, as much fun as this is, I'm excluding it from
            # the report.
            #
            # dedup_savings = aws_monthly_cost(managed_data_now) - storage_cost
            # <tr><th>Monthly savings from storage deduplication</th> <td>${dedup_savings:,.2f}</td></tr>
            add_row(wb_link('users', 'Total users'), self.total_users)
            add_row('Total projects', self.total_projects)
            add_row('Total data under management', bytes_si_fmt(managed_data_end))
            add_row('Total storage usage', bytes_si_fmt(storage_used_end))
            add_row('Deduplication ratio', dedup_ratio)
            add_row('Approximate monthly storage cost', cost_fmt(storage_cost_end))
            add_card(
                f'Cluster status as of {self.to.date()}',
                'See <a href="#prices">note on usage and cost calculations</a> for details on how costs are calculated.',
            )

        # We have a couple of options for getting total container hours
        #
        # total_hours=sum(v for _, v in containers_graph) / 12
        #
        # calculates the sum from prometheus metrics
        #
        # total_hours=self.total_hours
        #
        # calculates the sum of the containers that were fetched
        #
        # The problem is these numbers tend not to match, especially
        # if the report generation was not called with "include
        # workflow steps".
        #
        # I decided to use the sum from containers fetched, because it
        # will match the sum of compute time for each project listed
        # in the report.

        cluster_summary = self.summaries.pop(None)
        add_row('Active users', cluster_summary.total_users())
        add_row('<a href="#Active_Projects">Active projects</a>', len(self.summaries))
        add_row('Workflow runs', cluster_summary.total_runs())
        add_row('Compute used', duration_hrs_fmt(cluster_summary.total_runtime(), 'hours'))
        add_row('Compute cost', cost_fmt(cluster_summary.total_cost()))
        add_row(
            'Storage cost',
            cost_fmt(sum(s3_cost(v, S3CostPeriod.HOURLY) for _, v in storage_graph)),
        )
        add_card(
            'Activity and cost over the {} day period {} to {}'.format(
                (self.to - self.since).days, self.since.date(), self.to.date(),
            ),
            'See <a href="#prices">note on usage and cost calculations</a> for details on how costs are calculated.',
        )

        graphs = {
            ('Concurrent running containers', 'containers'): containers_graph,
            ('Data under management', 'managed'): managed_graph,
            ('Storage usage', 'used'): storage_graph,
        }
        if any(graphs.values()):
            cards.append("""
                <div id="chart"></div>
            """)

        projects = sorted(
            self.summaries.items(),
            key=lambda kv: kv[1].total_cost(),
            reverse=True,
        )
        project_rows = {
            key: '<td>{0}</td> <td>{1}</td> <td>{2}</td> <td>{3}</td>'.format(
                ', '.join(summary.usernames(self.users)),
                datespan_fmt(summary.earliest, summary.latest),
                duration_hrs_fmt(summary.total_runtime()),
                cost_fmt(summary.total_cost()),
            )
            for key, summary in projects
        }
        data_rows = '\n'.join(
            '<tr><td><a href="#{0}">{0}</a></td>{1}</tr>'.format(key, row)
            for key, row in project_rows.items()
        )
        cards.append(
            f"""
            <a id="Active_Projects"><h2>Active Projects</h2></a>
            <table class='sortable active-projects'>
            <thead><tr><th>Project</th> <th>Users</th> <th>Active</th> <th>Compute usage (hours)</th> <th>Compute cost</th> </tr></thead>
            <tbody>{data_rows}</tbody>
            </table>
            <p>See <a href="#prices">note on usage and cost calculations</a> for details on how costs are calculated.</p>
            """)

        for pkey, psum in projects:
            workflows = sorted(
                self.statistics[pkey].items(),
                key=lambda kv: kv[1].total_runs(),
                reverse=True,
            )
            data_rows = '\n                '.join(
                '<tr><td>{0}</td> <td>{1}</td> <td>{2}</td> <td>{3}</td> <td>{4}</td> <td>{5}</td> <td>{6}</td></tr>'.format(
                    stats.total_runs(),
                    wb_link(f'workflows/{stats.workflow_uuid}', key) if stats.workflow_uuid else key,
                    duration_hms_fmt(stats.median_runtime()),
                    duration_hms_fmt(stats.mean_runtime()),
                    cost_fmt(stats.median_cost()),
                    cost_fmt(stats.mean_cost()),
                    cost_fmt(stats.total_cost()),
                )
                for key, stats in workflows
            )
            title_link = wb_link(f'projects/{psum.owner_uuid}', pkey)
            cards.append(
                f"""<a id="{pkey}"></a><h2>{title_link}</h2>

                <table class='sortable single-project'>
                <thead><tr> <th>Users</th> <th>Active</th> <th>Compute usage (hours)</th> <th>Compute cost</th> </tr></thead>
                <tbody><tr>{project_rows[pkey]}</tr></tbody>
                </table>

                <table class='sortable project'>
                <thead><tr><th>Workflow run count</th> <th>Workflow name</th> <th>Median runtime</th> <th>Mean runtime</th> <th>Median cost per run</th> <th>Mean cost per run</th> <th>Sum cost over runs</th></tr></thead>
                <tbody>
                {data_rows}
                </tbody></table>
                """)

        # The deduplication ratio overstates things a bit, you can
        # have collections which reference a small slice of a large
        # block, and this messes up the intuitive value of this ratio
        # and exagerates the effect.
        #
        # So for now, as much fun as this is, I'm excluding it from
        # the report.
        #
        # <p>"Monthly savings from storage deduplication" is the
        # estimated cost difference between "storage usage" and "data
        # under management" as a way of comparing with other
        # technologies that do not support data deduplication.</p>


        cards.append("""
        <h2 id="prices">Note on usage and cost calculations</h2>

        <div style="max-width: 60em">

        <p>The numbers presented in this report are estimates and will
        not perfectly match your cloud bill.  Nevertheless this report
        should be useful for identifying your main cost drivers.</p>

        <h3>Storage</h3>

        <p>"Total data under management" is what you get if you add up
        all blocks referenced by all collections in Workbench, without
        considering deduplication.</p>

        <p>"Total storage usage" is the actual underlying storage
        usage, accounting for data deduplication.</p>

        <p>Storage costs are based on AWS "S3 Standard"
        described on the <a href="https://aws.amazon.com/s3/pricing/">Amazon S3 pricing</a> page:</p>

        <ul>
        <li>$0.023 per GB / Month for the first 50 TB</li>
        <li>$0.022 per GB / Month for the next 450 TB</li>
        <li>$0.021 per GB / Month over 500 TB</li>
        </ul>

        <p>Finally, this only the base storage cost, and does not
        include any fees associated with S3 API usage.  However, there
        are generally no ingress/egress fees if your Arvados instance
        and S3 bucket are in the same region, which is the normal
        recommended configuration.</p>

        <h3>Compute</h3>

        <p>"Compute usage" are instance-hours used in running
        workflows.  Because multiple steps may run in parallel on
        multiple instances, a workflow that completes in four hours
        but runs parallel steps on five instances, would be reported
        as using 20 instance hours.</p>

        <p>"Runtime" is the actual wall clock time that it took to
        complete a workflow.  This does not include time spent in the
        queue for the workflow itself, but does include queuing time
        of individual workflow steps.</p>

        <p>Computational costs are derived from Arvados cost
        calculations of container runs.  For on-demand instances, this
        uses the prices from the InstanceTypes section of the Arvado
        config file, set by the system administrator.  For spot
        instances, this uses current spot prices retrieved on the fly
        the AWS API.</p>

        <p>Be aware that the cost calculations are only for the time
        the container is running and only do not take into account the
        overhead of launching instances or idle time between scheduled
        tasks or prior to automatic shutdown.</p>

        </div>
        """)

        return ReportChart(
            "Cluster report for {} from {} to {}".format(
                self.cluster, self.since.date(), self.to.date(),
            ),
            cards,
            graphs,
        ).html()

    def csv_report(self, out, columns, *, include_steps: bool):
        if columns:
            columns = columns.split(",")
        elif include_steps:
            columns = ("Project", "Workflow", "Step", "Sample", "User", "Submitted", "Runtime", "Cost")
        else:
            columns = ("Project", "Workflow", "Sample", "User", "Submitted", "Runtime", "CumulativeCost")

        csvwriter = csv.DictWriter(out, fieldnames=columns, extrasaction="ignore")
        csvwriter.writeheader()
        csvwriter.writerows(run.csv_row() for run in self.iter_and_tally_runs())
        if include_steps:
            csvwriter.writerows(run.csv_row() for run in self.iter_steps())
