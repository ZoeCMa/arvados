# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging
import os
import sys
import threading
import copy
import json
import re
from functools import partial
import subprocess
import time
import urllib

from cwltool.errors import WorkflowException
import cwltool.workflow
from schema_salad.sourceline import SourceLine, cmap
import schema_salad.validate as validate
from schema_salad.ref_resolver import file_uri, uri_file_path

import arvados
import arvados.config
import arvados.util
from arvados.keep import KeepClient
from arvados.errors import ApiError

import arvados_cwl.util
from .arvcontainer import RunnerContainer, cleanup_name_for_collection
from .runner import Runner, upload_docker, upload_job_order, upload_workflow_deps, make_builder, update_from_merged_map, print_keep_deps, ArvSecretStore
from .arvtool import ArvadosCommandTool, validate_cluster_target, ArvadosExpressionTool
from .arvworkflow import ArvadosWorkflow, upload_workflow, make_workflow_record
from .fsaccess import CollectionFsAccess, CollectionFetcher, collectionResolver, CollectionCache, pdh_size
from .perf import Perf
from .pathmapper import NoFollowPathMapper
from cwltool.task_queue import TaskQueue
from .context import ArvLoadingContext, ArvRuntimeContext
from ._version import __version__

from cwltool.process import shortname, UnsupportedRequirement, use_custom_schema
from cwltool.utils import adjustFileObjs, adjustDirObjs, get_listing, visit_class, aslist
from cwltool.command_line_tool import compute_checksums
from cwltool.load_tool import load_tool

logger = logging.getLogger('arvados.cwl-runner')
metrics = logging.getLogger('arvados.cwl-runner.metrics')

DEFAULT_PRIORITY = 500

class RuntimeStatusLoggingHandler(logging.Handler):
    """
    Intercepts logging calls and report them as runtime statuses on runner
    containers.
    """
    def __init__(self, runtime_status_update_func):
        super(RuntimeStatusLoggingHandler, self).__init__()
        self.runtime_status_update = runtime_status_update_func
        self.updatingRuntimeStatus = False

    def emit(self, record):
        kind = None
        if record.levelno >= logging.ERROR:
            kind = 'error'
        elif record.levelno >= logging.WARNING:
            kind = 'warning'
        if kind == 'warning' and record.name in ("salad", "crunchstat_summary"):
            # Don't send validation warnings to runtime status,
            # they're noisy and unhelpful.
            return
        if kind is not None and self.updatingRuntimeStatus is not True:
            self.updatingRuntimeStatus = True
            try:
                log_msg = record.getMessage()
                if '\n' in log_msg:
                    # If the logged message is multi-line, use its first line as status
                    # and the rest as detail.
                    status, detail = log_msg.split('\n', 1)
                    self.runtime_status_update(
                        kind,
                        "%s: %s" % (record.name, status),
                        detail
                    )
                else:
                    self.runtime_status_update(
                        kind,
                        "%s: %s" % (record.name, record.getMessage())
                    )
            finally:
                self.updatingRuntimeStatus = False


class ArvCwlExecutor(object):
    """Execute a CWL tool or workflow, submit work (using containers API),
    wait for them to complete, and report output.

    """

    def __init__(self, api_client,
                 arvargs=None,
                 keep_client=None,
                 num_retries=4,
                 thread_count=4,
                 stdout=sys.stdout):

        if arvargs is None:
            arvargs = argparse.Namespace()
            arvargs.work_api = None
            arvargs.output_name = None
            arvargs.output_tags = None
            arvargs.thread_count = 1
            arvargs.collection_cache_size = None
            arvargs.git_info = True
            arvargs.submit = False
            arvargs.defer_downloads = False

        self.api = api_client
        self.processes = {}
        self.workflow_eval_lock = threading.Condition(threading.RLock())
        self.final_output = None
        self.final_status = None
        self.num_retries = num_retries
        self.uuid = None
        self.stop_polling = threading.Event()
        self.poll_api = None
        self.pipeline = None
        self.final_output_collection = None
        self.output_name = arvargs.output_name
        self.output_tags = arvargs.output_tags
        self.project_uuid = None
        self.intermediate_output_ttl = 0
        self.intermediate_output_collections = []
        self.trash_intermediate = False
        self.thread_count = arvargs.thread_count
        self.poll_interval = 12
        self.loadingContext = None
        self.should_estimate_cache_size = True
        self.fs_access = None
        self.secret_store = None
        self.stdout = stdout
        self.fast_submit = False
        self.git_info = arvargs.git_info
        self.debug = False
        self.botosession = None
        self.selected_credential = None

        if keep_client is not None:
            self.keep_client = keep_client
        else:
            self.keep_client = arvados.keep.KeepClient(api_client=self.api, num_retries=self.num_retries)

        if arvargs.collection_cache_size:
            collection_cache_size = arvargs.collection_cache_size*1024*1024
            self.should_estimate_cache_size = False
        else:
            collection_cache_size = 256*1024*1024

        self.collection_cache = CollectionCache(self.api, self.keep_client, self.num_retries,
                                                cap=collection_cache_size)

        self.fetcher_constructor = partial(CollectionFetcher,
                                           api_client=self.api,
                                           fs_access=CollectionFsAccess("", collection_cache=self.collection_cache),
                                           num_retries=self.num_retries)

        self.work_api = None
        expected_api = ["containers"]
        for api in expected_api:
            try:
                methods = self.api._rootDesc.get('resources')[api]['methods']
                if ('httpMethod' in methods['create'] and
                    (arvargs.work_api == api or arvargs.work_api is None)):
                    self.work_api = api
                    break
            except KeyError:
                pass

        if not self.work_api:
            if arvargs.work_api is None:
                raise Exception("No supported APIs")
            else:
                raise Exception("Unsupported API '%s', expected one of %s" % (arvargs.work_api, expected_api))

        if self.work_api == "jobs":
            logger.error("""
*******************************
The 'jobs' API is no longer supported.
*******************************""")
            exit(1)

        self.loadingContext = ArvLoadingContext(vars(arvargs))
        self.loadingContext.fetcher_constructor = self.fetcher_constructor
        self.loadingContext.resolver = partial(collectionResolver, self.api, num_retries=self.num_retries)
        self.loadingContext.construct_tool_object = self.arv_make_tool

        # Add a custom logging handler to the root logger for runtime status reporting
        # if running inside a container
        if arvados_cwl.util.get_current_container(self.api, self.num_retries, logger):
            root_logger = logging.getLogger('')

            # Remove existing RuntimeStatusLoggingHandlers if they exist
            handlers = [h for h in root_logger.handlers if not isinstance(h, RuntimeStatusLoggingHandler)]
            root_logger.handlers = handlers

            handler = RuntimeStatusLoggingHandler(self.runtime_status_update)
            root_logger.addHandler(handler)

        self.toplevel_runtimeContext = ArvRuntimeContext(vars(arvargs))
        self.toplevel_runtimeContext.make_fs_access = partial(CollectionFsAccess,
                                                     collection_cache=self.collection_cache)
        self.toplevel_runtimeContext.secret_store = ArvSecretStore()

        self.defer_downloads = arvargs.submit and arvargs.defer_downloads

        validate_cluster_target(self, self.toplevel_runtimeContext)


    def arv_make_tool(self, toolpath_object, loadingContext):
        if "class" in toolpath_object and toolpath_object["class"] == "CommandLineTool":
            return ArvadosCommandTool(self, toolpath_object, loadingContext)
        elif "class" in toolpath_object and toolpath_object["class"] == "Workflow":
            return ArvadosWorkflow(self, toolpath_object, loadingContext)
        elif "class" in toolpath_object and toolpath_object["class"] == "ExpressionTool":
            return ArvadosExpressionTool(self, toolpath_object, loadingContext)
        else:
            raise Exception("Unknown tool %s" % toolpath_object.get("class"))

    def output_callback(self, out, processStatus):
        with self.workflow_eval_lock:
            if processStatus == "success":
                logger.info("Overall process status is %s", processStatus)
                state = "Complete"
            else:
                logger.error("Overall process status is %s", processStatus)
                state = "Failed"
            if self.pipeline:
                self.api.pipeline_instances().update(uuid=self.pipeline["uuid"],
                                                        body={"state": state}).execute(num_retries=self.num_retries)
            self.final_status = processStatus
            self.final_output = out
            self.workflow_eval_lock.notify_all()


    def start_run(self, runnable, runtimeContext):
        self.task_queue.add(partial(runnable.run, runtimeContext),
                            self.workflow_eval_lock, self.stop_polling)

    def process_submitted(self, container):
        with self.workflow_eval_lock:
            self.processes[container.uuid] = container

    def process_done(self, uuid, record):
        with self.workflow_eval_lock:
            j = self.processes[uuid]
            logger.info("%s %s is %s", self.label(j), uuid, record["state"])
            self.task_queue.add(partial(j.done, record),
                                self.workflow_eval_lock, self.stop_polling)
            del self.processes[uuid]

    def runtime_status_update(self, kind, message, detail=None):
        """
        Updates the runtime_status field on the runner container.
        Called when there's a need to report errors, warnings or just
        activity statuses, for example in the RuntimeStatusLoggingHandler.
        """

        if kind not in ('error', 'warning', 'activity'):
            # Ignore any other status kind
            return

        with self.workflow_eval_lock:
            current = None
            try:
                current = arvados_cwl.util.get_current_container(self.api, self.num_retries, logger)
            except Exception as e:
                logger.info("Couldn't get current container: %s", e)
            if current is None:
                return
            runtime_status = current.get('runtime_status', {})

            original_updatemessage = updatemessage = runtime_status.get(kind, "")
            if kind == "activity" or not updatemessage:
                updatemessage = message

            # Subsequent messages tacked on in detail
            original_updatedetail = updatedetail = runtime_status.get(kind+'Detail', "")
            maxlines = 40
            if updatedetail.count("\n") < maxlines:
                if updatedetail:
                    updatedetail += "\n"
                updatedetail += message + "\n"

                if detail:
                    updatedetail += detail + "\n"

                if updatedetail.count("\n") >= maxlines:
                    updatedetail += "\nSome messages may have been omitted.  Check the full log."

            if updatemessage == original_updatemessage and updatedetail == original_updatedetail:
                # don't waste time doing an update if nothing changed
                # (usually because we exceeded the max lines)
                return

            runtime_status.update({
                kind: updatemessage,
                kind+'Detail': updatedetail,
            })

            try:
                self.api.containers().update(uuid=current['uuid'],
                                            body={
                                                'runtime_status': runtime_status,
                                            }).execute(num_retries=self.num_retries)
            except Exception as e:
                logger.info("Couldn't update runtime_status: %s", e)

    def wrapped_callback(self, cb, obj, st):
        with self.workflow_eval_lock:
            cb(obj, st)
            self.workflow_eval_lock.notifyAll()

    def get_wrapped_callback(self, cb):
        return partial(self.wrapped_callback, cb)

    def on_message(self, event):
        if event.get("object_uuid") in self.processes and event["event_type"] == "update":
            uuid = event["object_uuid"]
            if event["properties"]["new_attributes"]["state"] == "Running":
                with self.workflow_eval_lock:
                    j = self.processes[uuid]
                    if j.running is False:
                        j.running = True
                        j.update_pipeline_component(event["properties"]["new_attributes"])
                        logger.info("%s %s is Running", self.label(j), uuid)
            elif event["properties"]["new_attributes"]["state"] == "Final":
                # underlying container is completed or cancelled
                self.process_done(uuid, event["properties"]["new_attributes"])
            elif (event["properties"]["new_attributes"]["state"] == "Committed" and
                  event["properties"]["new_attributes"]["priority"] == 0):
                # cancelled before it got a chance to run, remains in
                # comitted state but isn't going to run so treat it as
                # cancelled.
                self.process_done(uuid, event["properties"]["new_attributes"])


    def label(self, obj):
        return "[%s %s]" % (self.work_api[0:-1], obj.name)

    def poll_states(self):
        """Poll status of containers listed in the processes dict.

        Runs in a separate thread.
        """

        try:
            remain_wait = self.poll_interval
            while True:
                if remain_wait > 0:
                    self.stop_polling.wait(remain_wait)
                if self.stop_polling.is_set():
                    break
                with self.workflow_eval_lock:
                    keys = list(self.processes)
                if not keys:
                    remain_wait = self.poll_interval
                    continue

                begin_poll = time.time()
                if self.work_api == "containers":
                    table = self.poll_api.container_requests()

                pageSize = self.poll_api._rootDesc.get('maxItemsPerResponse', 1000)

                while keys:
                    page = keys[:pageSize]
                    try:
                        proc_states = table.list(filters=[["uuid", "in", page]], select=["uuid", "container_uuid", "state", "log_uuid",
                                                                                         "output_uuid", "modified_at", "properties",
                                                                                         "runtime_constraints", "priority"]).execute(num_retries=self.num_retries)
                    except Exception as e:
                        logger.warning("Temporary error checking states on API server: %s", e)
                        remain_wait = self.poll_interval
                        continue

                    for p in proc_states["items"]:
                        self.on_message({
                            "object_uuid": p["uuid"],
                            "event_type": "update",
                            "properties": {
                                "new_attributes": p
                            }
                        })
                    keys = keys[pageSize:]

                finish_poll = time.time()
                remain_wait = self.poll_interval - (finish_poll - begin_poll)
        except:
            logger.exception("Fatal error in state polling thread.")
            with self.workflow_eval_lock:
                self.processes.clear()
                self.workflow_eval_lock.notifyAll()
        finally:
            self.stop_polling.set()

    def add_intermediate_output(self, uuid):
        if uuid:
            self.intermediate_output_collections.append(uuid)

    def trash_intermediate_output(self):
        logger.info("Cleaning up intermediate output collections")
        for i in self.intermediate_output_collections:
            try:
                self.api.collections().delete(uuid=i).execute(num_retries=self.num_retries)
            except Exception:
                logger.warning("Failed to delete intermediate output: %s", sys.exc_info()[1], exc_info=(sys.exc_info()[1] if self.debug else False))
            except (KeyboardInterrupt, SystemExit):
                break

    def check_features(self, obj, parentfield=""):
        if isinstance(obj, dict):
            if obj.get("class") == "DockerRequirement":
                if obj.get("dockerOutputDirectory"):
                    if not obj.get("dockerOutputDirectory").startswith('/'):
                        raise SourceLine(obj, "dockerOutputDirectory", validate.ValidationException).makeError(
                            "Option 'dockerOutputDirectory' must be an absolute path.")
            if obj.get("class") == "InplaceUpdateRequirement":
                if obj["inplaceUpdate"] and parentfield == "requirements":
                    raise SourceLine(obj, "class", UnsupportedRequirement).makeError("InplaceUpdateRequirement not supported for keep collections.")
            for k,v in obj.items():
                self.check_features(v, parentfield=k)
        elif isinstance(obj, list):
            for i,v in enumerate(obj):
                with SourceLine(obj, i, UnsupportedRequirement, logger.isEnabledFor(logging.DEBUG)):
                    self.check_features(v, parentfield=parentfield)

    def make_output_collection(self, name, storage_classes, tagsString, output_properties, outputObj):
        outputObj = copy.deepcopy(outputObj)

        files = []
        def captureFile(fileobj):
            files.append(fileobj)

        def captureDir(dirobj):
            if dirobj["location"].startswith("keep:") and 'listing' in dirobj:
                del dirobj['listing']
            files.append(dirobj)

        adjustDirObjs(outputObj, captureDir)
        adjustFileObjs(outputObj, captureFile)

        generatemapper = NoFollowPathMapper(files, "", "", separateDirs=False)

        final = arvados.collection.Collection(api_client=self.api,
                                              keep_client=self.keep_client,
                                              num_retries=self.num_retries)

        for k,v in generatemapper.items():
            if v.type == "Directory" and v.resolved.startswith("_:"):
                    continue
            if v.type == "CreateFile" and (k.startswith("_:") or v.resolved.startswith("_:")):
                with final.open(v.target, "wb") as f:
                    f.write(v.resolved.encode("utf-8"))
                    continue

            if not v.resolved.startswith("keep:"):
                raise Exception("Output source is not in keep or a literal")
            sp = v.resolved.split("/")
            srccollection = sp[0][5:]
            try:
                reader = self.collection_cache.get(srccollection)
                srcpath = urllib.parse.unquote("/".join(sp[1:]) if len(sp) > 1 else ".")
                final.copy(srcpath, v.target, source_collection=reader, overwrite=False)
            except arvados.errors.ArgumentError as e:
                logger.error("Creating CollectionReader for '%s' '%s': %s", k, v, e)
                raise
            except IOError as e:
                logger.error("While preparing output collection: %s", e)
                raise

        def rewrite(fileobj):
            fileobj["location"] = generatemapper.mapper(fileobj["location"]).target
            for k in ("listing", "contents", "nameext", "nameroot", "dirname"):
                if k in fileobj:
                    del fileobj[k]

        adjustDirObjs(outputObj, rewrite)
        adjustFileObjs(outputObj, rewrite)

        with final.open("cwl.output.json", "w") as f:
            res = str(json.dumps(outputObj, sort_keys=True, indent=4, separators=(',',': '), ensure_ascii=False))
            f.write(res)


        final.save_new(name=name, owner_uuid=self.project_uuid, storage_classes=storage_classes,
                       ensure_unique_name=True, properties=output_properties)

        logger.info("Final output collection %s \"%s\" (%s)", final.portable_data_hash(),
                    final.api_response()["name"],
                    final.manifest_locator())

        final_uuid = final.manifest_locator()
        tags = tagsString.split(',')
        for tag in tags:
             self.api.links().create(body={
                "head_uuid": final_uuid, "link_class": "tag", "name": tag
                }).execute(num_retries=self.num_retries)

        def finalcollection(fileobj):
            fileobj["location"] = "keep:%s/%s" % (final.portable_data_hash(), fileobj["location"])

        adjustDirObjs(outputObj, finalcollection)
        adjustFileObjs(outputObj, finalcollection)

        return (outputObj, final)

    def set_crunch_output(self):
        if self.work_api == "containers":
            current = arvados_cwl.util.get_current_container(self.api, self.num_retries, logger)
            if current is None:
                return
            try:
                self.api.containers().update(uuid=current['uuid'],
                                             body={
                                                 'output': self.final_output_collection.portable_data_hash(),
                                                 'output_properties': self.final_output_collection.get_properties(),
                                             }).execute(num_retries=self.num_retries)
                self.api.collections().update(uuid=self.final_output_collection.manifest_locator(),
                                              body={
                                                  'is_trashed': True
                                              }).execute(num_retries=self.num_retries)
            except Exception:
                logger.exception("Setting container output")
                raise

    def apply_reqs(self, job_order_object, tool):
        if "https://w3id.org/cwl/cwl#requirements" in job_order_object:
            if tool.metadata.get("http://commonwl.org/cwltool#original_cwlVersion") == 'v1.0':
                raise WorkflowException(
                    "`cwl:requirements` in the input object is not part of CWL "
                    "v1.0. You can adjust to use `cwltool:overrides` instead; or you "
                    "can set the cwlVersion to v1.1 or greater and re-run with "
                    "--enable-dev.")
            job_reqs = job_order_object["https://w3id.org/cwl/cwl#requirements"]
            for req in job_reqs:
                tool.requirements.append(req)

    @staticmethod
    def get_git_info(tool):
        in_a_git_repo = False
        cwd = None
        filepath = None

        if tool.tool["id"].startswith("file://"):
            # check if git is installed
            try:
                filepath = uri_file_path(tool.tool["id"])
                cwd = os.path.dirname(filepath)
                subprocess.run(
                    ["git", "log", "--format=%H", "-n1", "HEAD"],
                    cwd=cwd,
                    check=True,
                    stdout=subprocess.DEVNULL,
                )
                in_a_git_repo = True
            except Exception as e:
                pass

        gitproperties = {}

        if in_a_git_repo:
            def git_output(cmd):
                return subprocess.run(
                    cmd,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                ).stdout.strip()
            git_commit = git_output(["git", "log", "--format=%H", "-n1", "HEAD"])
            git_date = git_output(["git", "log", "--format=%cD", "-n1", "HEAD"])
            git_committer = git_output(["git", "log", "--format=%cn <%ce>", "-n1", "HEAD"])
            git_branch = git_output(["git", "rev-parse", "--abbrev-ref", "HEAD"])
            git_origin = git_output(["git", "remote", "get-url", "origin"])
            git_status = git_output(["git", "status", "--untracked-files=no", "--porcelain"])
            git_describe = git_output(["git", "describe", "--always", "--tags"])
            git_toplevel = git_output(["git", "rev-parse", "--show-toplevel"])
            git_path = filepath[len(git_toplevel):]

            git_origin = arvados_cwl.util.sanitize_url(git_origin)

            gitproperties = {
                "http://arvados.org/cwl#gitCommit": git_commit,
                "http://arvados.org/cwl#gitDate": git_date,
                "http://arvados.org/cwl#gitCommitter": git_committer,
                "http://arvados.org/cwl#gitBranch": git_branch,
                "http://arvados.org/cwl#gitOrigin": git_origin,
                "http://arvados.org/cwl#gitStatus": git_status,
                "http://arvados.org/cwl#gitDescribe": git_describe,
                "http://arvados.org/cwl#gitPath": git_path,
            }
        else:
            for g in ("http://arvados.org/cwl#gitCommit",
                      "http://arvados.org/cwl#gitDate",
                      "http://arvados.org/cwl#gitCommitter",
                      "http://arvados.org/cwl#gitBranch",
                      "http://arvados.org/cwl#gitOrigin",
                      "http://arvados.org/cwl#gitStatus",
                      "http://arvados.org/cwl#gitDescribe",
                      "http://arvados.org/cwl#gitPath"):
                if g in tool.metadata:
                    gitproperties[g] = tool.metadata[g]

        return gitproperties

    def set_container_request_properties(self, container, properties):
        resp = self.api.container_requests().list(filters=[["container_uuid", "=", container["uuid"]]], select=["uuid", "properties"]).execute(num_retries=self.num_retries)
        for cr in resp["items"]:
            cr["properties"].update({k.replace("http://arvados.org/cwl#", "arv:"): v for k, v in properties.items()})
            self.api.container_requests().update(uuid=cr["uuid"], body={"container_request": {"properties": cr["properties"]}}).execute(num_retries=self.num_retries)

    def get_credential(self, runtimeContext):
        if runtimeContext.selected_credential is None:
            return

        for key in ("uuid", "name"):
            result = self.api.credentials().list(filters=[[key, "=", runtimeContext.selected_credential]]).execute()
            if len(result["items"]) == 1:
                self.selected_credential = result["items"][0]
                break

    def get_credential_secret(self):
        if self.selected_credential is None:
            return
        self.selected_credential.update(self.api.credentials().secret(uuid=self.selected_credential["uuid"]).execute())


    def arv_executor(self, updated_tool, job_order, runtimeContext, logger=None):
        self.debug = runtimeContext.debug

        self.runtime_status_update("activity", "initialization")

        git_info = self.get_git_info(updated_tool) if self.git_info else {}
        if git_info:
            logger.info("Git provenance")
            for g in git_info:
                if git_info[g]:
                    logger.info("  %s: %s", g.split("#", 1)[1], git_info[g])

        runtimeContext.git_info = git_info

        workbench1 = self.api.config()["Services"]["Workbench1"]["ExternalURL"]
        workbench2 = self.api.config()["Services"]["Workbench2"]["ExternalURL"]
        controller = self.api.config()["Services"]["Controller"]["ExternalURL"]
        logger.info("Using cluster %s (%s)", self.api.config()["ClusterID"], workbench2 or workbench1 or controller)

        if not self.fast_submit:
            updated_tool.visit(self.check_features)

        self.pipeline = None
        self.fs_access = runtimeContext.make_fs_access(runtimeContext.basedir)
        self.secret_store = runtimeContext.secret_store

        self.trash_intermediate = runtimeContext.trash_intermediate
        if self.trash_intermediate and self.work_api != "containers":
            raise Exception("--trash-intermediate is only supported with --api=containers.")

        self.intermediate_output_ttl = runtimeContext.intermediate_output_ttl
        if self.intermediate_output_ttl and self.work_api != "containers":
            raise Exception("--intermediate-output-ttl is only supported with --api=containers.")
        if self.intermediate_output_ttl < 0:
            raise Exception("Invalid value %d for --intermediate-output-ttl, cannot be less than zero" % self.intermediate_output_ttl)

        if runtimeContext.submit_request_uuid and self.work_api != "containers":
            raise Exception("--submit-request-uuid requires containers API, but using '{}' api".format(self.work_api))

        runtimeContext = runtimeContext.copy()

        if not runtimeContext.name:
            self.name = updated_tool.tool.get("label") or updated_tool.metadata.get("label") or os.path.basename(updated_tool.tool["id"])
            if git_info.get("http://arvados.org/cwl#gitDescribe"):
                self.name = "%s (%s)" % (self.name, git_info.get("http://arvados.org/cwl#gitDescribe"))
            runtimeContext.name = self.name

        if runtimeContext.copy_deps is None and (runtimeContext.create_workflow or runtimeContext.update_workflow):
            # When creating or updating workflow record, by default
            # always copy dependencies and ensure Docker images are up
            # to date.
            runtimeContext.copy_deps = True
            runtimeContext.match_local_docker = True

        if runtimeContext.print_keep_deps:
            runtimeContext.copy_deps = False
            runtimeContext.match_local_docker = False

        if runtimeContext.update_workflow and self.project_uuid is None:
            # If we are updating a workflow, make sure anything that
            # gets uploaded goes into the same parent project, unless
            # an alternate --project-uuid was provided.
            existing_wf = self.api.workflows().get(uuid=runtimeContext.update_workflow).execute()
            runtimeContext.project_uuid = existing_wf["owner_uuid"]

        self.project_uuid = runtimeContext.project_uuid

        self.runtime_status_update("activity", "data transfer")

        current_container = arvados_cwl.util.get_current_container(self.api, self.num_retries, logger)
        self.get_credential(runtimeContext)
        if current_container:
            logger.info("Running inside container %s", current_container.get("uuid"))
            self.get_credential_secret()

        # Upload local file references in the job order.
        with Perf(metrics, "upload_job_order"):
            job_order, jobmapper = upload_job_order(self, "%s input" % runtimeContext.name,
                                         updated_tool, job_order, runtimeContext)

        # determine if we are submitting or directly executing the workflow.
        #
        # the last clause means: if it is a command line tool, and we
        # are going to wait for the result, and always_submit_runner
        # is false, then we don't submit a runner process.

        submitting = ((runtimeContext.submit and not
                       (updated_tool.tool["class"] == "CommandLineTool" and
                        runtimeContext.wait and
                        not runtimeContext.always_submit_runner)) or
                      runtimeContext.defer_downloads)

        loadingContext = self.loadingContext.copy()
        loadingContext.do_validate = False
        loadingContext.disable_js_validation = True
        tool = updated_tool

        # Upload direct dependencies of workflow steps, get back mapping of files to keep references.
        # Also uploads docker images.
        if not self.fast_submit:
            logger.info("Uploading workflow dependencies")
            with Perf(metrics, "upload_workflow_deps"):
                merged_map = upload_workflow_deps(self, tool, runtimeContext)
        else:
            # in the fast submit case, we are running a workflow that
            # has already been uploaded to Arvados, so we assume all
            # the dependencies have been pinned to keep references and
            # there is nothing to do.
            merged_map = {}

        loadingContext.loader = tool.doc_loader
        loadingContext.avsc_names = tool.doc_schema
        loadingContext.metadata = tool.metadata
        loadingContext.skip_resolve_all = True

        workflow_wrapper = None
        if (submitting and not self.fast_submit) or runtimeContext.update_workflow or runtimeContext.create_workflow or runtimeContext.print_keep_deps:
            # upload workflow and get back the workflow wrapper

            workflow_wrapper = upload_workflow(self, tool, job_order,
                                               runtimeContext.project_uuid,
                                               runtimeContext,
                                               uuid=runtimeContext.update_workflow,
                                               submit_runner_ram=runtimeContext.submit_runner_ram,
                                               name=runtimeContext.name,
                                               merged_map=merged_map,
                                               submit_runner_image=runtimeContext.submit_runner_image,
                                               git_info=git_info,
                                               set_defaults=(runtimeContext.update_workflow or runtimeContext.create_workflow),
                                               jobmapper=jobmapper)

            if runtimeContext.update_workflow or runtimeContext.create_workflow:
                # We're registering the workflow, so create or update
                # the workflow record and then exit.
                uuid = make_workflow_record(self, workflow_wrapper, runtimeContext.name, tool,
                                            runtimeContext.project_uuid, runtimeContext.update_workflow)
                self.stdout.write(uuid + "\n")
                return (None, "success")

            if runtimeContext.print_keep_deps:
                # Just find and print out all the collection dependencies and exit
                print_keep_deps(self, runtimeContext, merged_map, tool)
                return (None, "success")

            # Did not register a workflow, we're going to submit
            # it instead.
            loadingContext.loader.idx.clear()
            loadingContext.loader.idx["_:main"] = workflow_wrapper
            workflow_wrapper["id"] = "_:main"

            # Reload the minimal wrapper workflow.
            self.fast_submit = True
            tool = load_tool(workflow_wrapper, loadingContext)
            loadingContext.loader.idx["_:main"] = workflow_wrapper

        if not submitting:
            # If we are going to run the workflow now (rather than
            # submit it), we need to update the workflow document
            # replacing file references with keep references.  If we
            # are just going to construct a run submission, we don't
            # need to do this.
            update_from_merged_map(tool, merged_map)

        self.apply_reqs(job_order, tool)

        self.ignore_docker_for_reuse = runtimeContext.ignore_docker_for_reuse
        self.eval_timeout = runtimeContext.eval_timeout

        runtimeContext.use_container = True
        runtimeContext.tmpdir_prefix = "tmp"
        runtimeContext.work_api = self.work_api

        if not self.output_name:
             self.output_name = "Output from workflow %s" % runtimeContext.name

        self.output_name  = cleanup_name_for_collection(self.output_name)

        if self.work_api == "containers":
            if self.ignore_docker_for_reuse:
                raise Exception("--ignore-docker-for-reuse not supported with containers API.")
            runtimeContext.outdir = "/var/spool/cwl"
            runtimeContext.docker_outdir = "/var/spool/cwl"
            runtimeContext.tmpdir = "/tmp"
            runtimeContext.docker_tmpdir = "/tmp"

        if runtimeContext.priority < 1 or runtimeContext.priority > 1000:
            raise Exception("--priority must be in the range 1..1000.")

        if self.should_estimate_cache_size:
            visited = set()
            estimated_size = [0]
            def estimate_collection_cache(obj):
                if obj.get("location", "").startswith("keep:"):
                    m = pdh_size.match(obj["location"][5:])
                    if m and m.group(1) not in visited:
                        visited.add(m.group(1))
                        estimated_size[0] += int(m.group(2))
            visit_class(job_order, ("File", "Directory"), estimate_collection_cache)
            runtimeContext.collection_cache_size = max(((estimated_size[0]*192) // (1024*1024))+1, 256)
            self.collection_cache.set_cap(runtimeContext.collection_cache_size*1024*1024)

        logger.info("Using collection cache size %s MiB", runtimeContext.collection_cache_size)

        runnerjob = None
        if runtimeContext.submit:
            # We are submitting instead of running immediately.
            #
            # Create a "Runner job" that when run() is invoked,
            # creates the container request to run the workflow.
            if self.work_api == "containers":
                if submitting:
                    loadingContext.metadata = updated_tool.metadata.copy()
                    tool = RunnerContainer(self, tool, loadingContext, runtimeContext.enable_reuse,
                                           self.output_name,
                                           self.output_tags,
                                           submit_runner_ram=runtimeContext.submit_runner_ram,
                                           name=runtimeContext.name,
                                           on_error=runtimeContext.on_error,
                                           submit_runner_image=runtimeContext.submit_runner_image,
                                           intermediate_output_ttl=runtimeContext.intermediate_output_ttl,
                                           merged_map=merged_map,
                                           priority=runtimeContext.priority,
                                           secret_store=self.secret_store,
                                           collection_cache_size=runtimeContext.collection_cache_size,
                                           collection_cache_is_default=self.should_estimate_cache_size,
                                           git_info=git_info)
                else:
                    runtimeContext.runnerjob = tool.tool["id"]

        if runtimeContext.cwl_runner_job is not None:
            self.uuid = runtimeContext.cwl_runner_job.get('uuid')

        jobiter = tool.job(job_order,
                           self.output_callback,
                           runtimeContext)

        if runtimeContext.submit and not runtimeContext.wait:
            # User provided --no-wait so submit the container request,
            # get the container request uuid, print it out, and exit.
            runnerjob = next(jobiter)
            runnerjob.run(runtimeContext)
            self.stdout.write(runnerjob.uuid+"\n")
            return (None, "success")

        # We either running the workflow directly, or submitting it
        # and will wait for a final result.

        self.runtime_status_update("activity", "workflow execution")

        if current_container:
            self.set_container_request_properties(current_container, git_info)

        self.poll_api = arvados.api('v1', timeout=runtimeContext.http_timeout)
        self.polling_thread = threading.Thread(target=self.poll_states)
        self.polling_thread.start()

        self.task_queue = TaskQueue(self.workflow_eval_lock, self.thread_count)

        try:
            self.workflow_eval_lock.acquire()

            # Holds the lock while this code runs and releases it when
            # it is safe to do so in self.workflow_eval_lock.wait(),
            # at which point on_message can update job state and
            # process output callbacks.

            loopperf = Perf(metrics, "jobiter")
            loopperf.__enter__()
            for runnable in jobiter:
                loopperf.__exit__()

                if self.stop_polling.is_set():
                    break

                if self.task_queue.error is not None:
                    raise self.task_queue.error

                if runnable:
                    with Perf(metrics, "run"):
                        self.start_run(runnable, runtimeContext)
                else:
                    if (self.task_queue.in_flight + len(self.processes)) > 0:
                        self.workflow_eval_lock.wait(3)
                    else:
                        if self.final_status is None:
                            logger.error("Workflow is deadlocked, no runnable processes and not waiting on any pending processes.")
                        break

                if self.stop_polling.is_set():
                    break

                loopperf.__enter__()
            loopperf.__exit__()

            while (self.task_queue.in_flight + len(self.processes)) > 0:
                if self.task_queue.error is not None:
                    raise self.task_queue.error
                self.workflow_eval_lock.wait(3)

        except UnsupportedRequirement:
            raise
        except:
            if sys.exc_info()[0] is KeyboardInterrupt or sys.exc_info()[0] is SystemExit:
                logger.error("Interrupted, workflow will be cancelled", exc_info=self.debug)
            elif isinstance(sys.exc_info()[1], WorkflowException):
                logger.error("Workflow execution failed:\n%s", sys.exc_info()[1], exc_info=self.debug)
            else:
                logger.exception("Workflow execution failed")

            if self.pipeline:
                self.api.pipeline_instances().update(uuid=self.pipeline["uuid"],
                                                     body={"state": "Failed"}).execute(num_retries=self.num_retries)

            if self.work_api == "containers" and not current_container:
                # Not running in a crunch container, so cancel any outstanding processes.
                for p in self.processes:
                    try:
                        self.api.container_requests().update(uuid=p,
                                                             body={"priority": "0"}
                        ).execute(num_retries=self.num_retries)
                    except Exception:
                        pass
        finally:
            self.workflow_eval_lock.release()
            self.task_queue.drain()
            self.stop_polling.set()
            self.polling_thread.join()
            self.task_queue.join()

        if self.final_status == "UnsupportedRequirement":
            raise UnsupportedRequirement("Check log for details.")

        if self.final_output is None:
            raise WorkflowException("Workflow did not return a result.")

        if runtimeContext.usage_report_notes:
            logger.info("Steps with low resource utilization (possible optimization opportunities):")
            for x in runtimeContext.usage_report_notes:
                logger.info("  %s", x)

        if runtimeContext.submit and isinstance(tool, Runner):
            logger.info("Final output collection %s", tool.final_output)
            if workbench2 or workbench1:
                logger.info("Output at %scollections/%s", workbench2 or workbench1, tool.final_output)
        else:
            if self.output_tags is None:
                self.output_tags = ""

            storage_classes = ""
            storage_class_req, _ = tool.get_requirement("http://arvados.org/cwl#OutputStorageClass")
            if storage_class_req and storage_class_req.get("finalStorageClass"):
                storage_classes = aslist(storage_class_req["finalStorageClass"])
            else:
                storage_classes = (
                    runtimeContext.storage_classes
                    or list(arvados.util.iter_storage_classes(self.api.config()))
                )

            output_properties = {}
            output_properties_req, _ = tool.get_requirement("http://arvados.org/cwl#OutputCollectionProperties")
            if output_properties_req:
                builder = make_builder(job_order, tool.hints, tool.requirements, runtimeContext, tool.metadata)
                for pr in output_properties_req["outputProperties"]:
                    output_properties[pr["propertyName"]] = builder.do_eval(pr["propertyValue"])

            self.final_output, self.final_output_collection = self.make_output_collection(self.output_name, storage_classes,
                                                                                          self.output_tags, output_properties,
                                                                                          self.final_output)
            self.set_crunch_output()

        if runtimeContext.compute_checksum:
            adjustDirObjs(self.final_output, partial(get_listing, self.fs_access))
            adjustFileObjs(self.final_output, partial(compute_checksums, self.fs_access))

        if self.trash_intermediate and self.final_status == "success":
            self.trash_intermediate_output()

        return (self.final_output, self.final_status)

def blank_secrets(job_order_object, process):
    secrets_req, _ = process.get_requirement("http://commonwl.org/cwltool#Secrets")
    pass
