# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

import logging
import json
import os
import urllib.request, urllib.parse, urllib.error
import time
import datetime
import ciso8601
import uuid
import math
import re

import arvados_cwl.util
import ruamel.yaml

from cwltool.errors import WorkflowException
from cwltool.process import UnsupportedRequirement, shortname
from cwltool.utils import aslist, adjustFileObjs, adjustDirObjs, visit_class
from cwltool.job import JobBase
from cwltool.builder import substitute

import arvados.collection
import arvados.util

import crunchstat_summary.summarizer
import crunchstat_summary.reader

from .arvdocker import arv_docker_get_image
from . import done
from .runner import Runner, arvados_jobs_image, packed_workflow, trim_anonymous_location, remove_redundant_fields, make_builder
from .fsaccess import CollectionFetcher
from .pathmapper import NoFollowPathMapper, trim_listing
from .perf import Perf
from ._version import __version__

logger = logging.getLogger('arvados.cwl-runner')
metrics = logging.getLogger('arvados.cwl-runner.metrics')

def cleanup_name_for_collection(name):
    return name.replace("/", " ")

class OutputGlobError(RuntimeError):
    pass

class ArvadosContainer(JobBase):
    """Submit and manage a Crunch container request for executing a CWL CommandLineTool."""

    def __init__(self, runner, job_runtime, globpatterns,
                 builder,   # type: Builder
                 joborder,  # type: Dict[Text, Union[Dict[Text, Any], List, Text]]
                 make_path_mapper,  # type: Callable[..., PathMapper]
                 requirements,      # type: List[Dict[Text, Text]]
                 hints,     # type: List[Dict[Text, Text]]
                 name       # type: Text
    ):
        super(ArvadosContainer, self).__init__(builder, joborder, make_path_mapper, requirements, hints, name)
        self.arvrunner = runner
        self.job_runtime = job_runtime
        self.running = False
        self.uuid = None
        self.attempt_count = 0
        self.globpatterns = globpatterns

    def update_pipeline_component(self, r):
        pass

    def _required_env(self):
        env = {}
        env["HOME"] = self.outdir
        env["TMPDIR"] = self.tmpdir
        return env

    def run(self, toplevelRuntimeContext):
        # ArvadosCommandTool subclasses from cwltool.CommandLineTool,
        # which calls makeJobRunner() to get a new ArvadosContainer
        # object.  The fields that define execution such as
        # command_line, environment, etc are set on the
        # ArvadosContainer object by CommandLineTool.job() before
        # run() is called.

        runtimeContext = self.job_runtime

        if runtimeContext.submit_request_uuid:
            container_request = self.arvrunner.api.container_requests().get(
                uuid=runtimeContext.submit_request_uuid
            ).execute(num_retries=self.arvrunner.num_retries)
        else:
            container_request = {}

        container_request["command"] = self.command_line
        container_request["name"] = self.name
        container_request["output_path"] = self.outdir
        container_request["cwd"] = self.outdir
        container_request["priority"] = runtimeContext.priority
        container_request["state"] = "Uncommitted"
        container_request.setdefault("properties", {})

        container_request["properties"]["cwl_input"] = self.joborder

        runtime_constraints = {}

        if runtimeContext.project_uuid:
            container_request["owner_uuid"] = runtimeContext.project_uuid

        if self.arvrunner.secret_store.has_secret(self.command_line):
            raise WorkflowException("Secret material leaked on command line, only file literals may contain secrets")

        if self.arvrunner.secret_store.has_secret(self.environment):
            raise WorkflowException("Secret material leaked in environment, only file literals may contain secrets")

        resources = self.builder.resources
        if resources is not None:
            runtime_constraints["vcpus"] = math.ceil(resources.get("cores", 1))
            runtime_constraints["ram"] = math.ceil(resources.get("ram") * 2**20)

        mounts = {
            self.outdir: {
                "kind": "tmp",
                "capacity": math.ceil(resources.get("outdirSize", 0) * 2**20)
            },
            self.tmpdir: {
                "kind": "tmp",
                "capacity": math.ceil(resources.get("tmpdirSize", 0) * 2**20)
            }
        }
        secret_mounts = {}
        scheduling_parameters = {}

        rf = [self.pathmapper.mapper(f) for f in self.pathmapper.referenced_files]
        rf.sort(key=lambda k: k.resolved)
        prevdir = None
        for resolved, target, tp, stg in rf:
            if not stg:
                continue
            if prevdir and target.startswith(prevdir):
                continue
            if tp == "Directory":
                targetdir = target
            else:
                targetdir = os.path.dirname(target)
            sp = resolved.split("/", 1)
            pdh = sp[0][5:]   # remove "keep:"
            mounts[targetdir] = {
                "kind": "collection",
                "portable_data_hash": pdh
            }
            if pdh in self.pathmapper.pdh_to_uuid:
                mounts[targetdir]["uuid"] = self.pathmapper.pdh_to_uuid[pdh]
            if len(sp) == 2:
                if tp == "Directory":
                    path = sp[1]
                else:
                    path = os.path.dirname(sp[1])
                if path and path != "/":
                    mounts[targetdir]["path"] = path
            prevdir = targetdir + "/"

        intermediate_collection_info = arvados_cwl.util.get_intermediate_collection_info(self.name, runtimeContext.current_container, runtimeContext.intermediate_output_ttl)

        with Perf(metrics, "generatefiles %s" % self.name):
            if self.generatefiles["listing"]:
                vwd = arvados.collection.Collection(api_client=self.arvrunner.api,
                                                    keep_client=self.arvrunner.keep_client,
                                                    num_retries=self.arvrunner.num_retries)
                generatemapper = NoFollowPathMapper(self.generatefiles["listing"], "", "",
                                                    separateDirs=False)

                sorteditems = sorted(generatemapper.items(), key=lambda n: n[1].target)

                logger.debug("generatemapper is %s", sorteditems)

                with Perf(metrics, "createfiles %s" % self.name):
                    for f, p in sorteditems:
                        if not p.target:
                            continue

                        if p.target.startswith("/"):
                            dst = p.target[len(self.outdir)+1:] if p.target.startswith(self.outdir+"/") else p.target[1:]
                        else:
                            dst = p.target

                        if p.type in ("File", "Directory", "WritableFile", "WritableDirectory"):
                            if p.resolved.startswith("_:"):
                                vwd.mkdirs(dst)
                            else:
                                source, path = self.arvrunner.fs_access.get_collection(p.resolved)
                                vwd.copy(path or ".", dst, source_collection=source)
                        elif p.type == "CreateFile":
                            if self.arvrunner.secret_store.has_secret(p.resolved):
                                mountpoint = p.target if p.target.startswith("/") else os.path.join(self.outdir, p.target)
                                secret_mounts[mountpoint] = {
                                    "kind": "text",
                                    "content": self.arvrunner.secret_store.retrieve(p.resolved)
                                }
                            else:
                                with vwd.open(dst, "w") as n:
                                    n.write(p.resolved)

                def keepemptydirs(p):
                    if isinstance(p, arvados.collection.RichCollectionBase):
                        if len(p) == 0:
                            p.open(".keep", "w").close()
                        else:
                            for c in p:
                                keepemptydirs(p[c])

                keepemptydirs(vwd)

                if not runtimeContext.current_container:
                    runtimeContext.current_container = arvados_cwl.util.get_current_container(self.arvrunner.api, self.arvrunner.num_retries, logger)
                vwd.save_new(name=intermediate_collection_info["name"],
                             owner_uuid=runtimeContext.project_uuid,
                             ensure_unique_name=True,
                             trash_at=intermediate_collection_info["trash_at"],
                             properties=intermediate_collection_info["properties"])

                prev = None
                for f, p in sorteditems:
                    if (not p.target or self.arvrunner.secret_store.has_secret(p.resolved) or
                        (prev is not None and p.target.startswith(prev))):
                        continue
                    if p.target.startswith("/"):
                        dst = p.target[len(self.outdir)+1:] if p.target.startswith(self.outdir+"/") else p.target[1:]
                    else:
                        dst = p.target
                    mountpoint = p.target if p.target.startswith("/") else os.path.join(self.outdir, p.target)
                    mounts[mountpoint] = {"kind": "collection",
                                          "portable_data_hash": vwd.portable_data_hash(),
                                          "path": dst}
                    if p.type.startswith("Writable"):
                        mounts[mountpoint]["writable"] = True
                    prev = p.target + "/"

        container_request["environment"] = {"TMPDIR": self.tmpdir, "HOME": self.outdir}
        if self.environment:
            container_request["environment"].update(self.environment)

        if self.stdin:
            sp = self.stdin[6:].split("/", 1)
            mounts["stdin"] = {"kind": "collection",
                                "portable_data_hash": sp[0],
                                "path": sp[1]}

        if self.stderr:
            mounts["stderr"] = {"kind": "file",
                                "path": "%s/%s" % (self.outdir, self.stderr)}

        if self.stdout:
            mounts["stdout"] = {"kind": "file",
                                "path": "%s/%s" % (self.outdir, self.stdout)}

        (docker_req, docker_is_req) = self.get_requirement("DockerRequirement")

        container_request["container_image"] = arv_docker_get_image(self.arvrunner.api,
                                                                    docker_req,
                                                                    runtimeContext.pull_image,
                                                                    runtimeContext)

        network_req, _ = self.get_requirement("NetworkAccess")
        if network_req:
            runtime_constraints["API"] = network_req["networkAccess"]

        api_req, _ = self.get_requirement("http://arvados.org/cwl#APIRequirement")
        if api_req:
            runtime_constraints["API"] = True

        use_disk_cache = (self.arvrunner.api.config()["Containers"].get("DefaultKeepCacheRAM", 0) == 0)

        keep_cache_type_req, _ = self.get_requirement("http://arvados.org/cwl#KeepCacheTypeRequirement")
        if keep_cache_type_req:
            if "keepCacheType" in keep_cache_type_req:
                if keep_cache_type_req["keepCacheType"] == "ram_cache":
                    use_disk_cache = False

        runtime_req, _ = self.get_requirement("http://arvados.org/cwl#RuntimeConstraints")
        if runtime_req:
            if "keep_cache" in runtime_req:
                if use_disk_cache:
                    # If DefaultKeepCacheRAM is zero it means we should use disk cache.
                    runtime_constraints["keep_cache_disk"] = math.ceil(runtime_req["keep_cache"] * 2**20)
                else:
                    runtime_constraints["keep_cache_ram"] = math.ceil(runtime_req["keep_cache"] * 2**20)
            if "outputDirType" in runtime_req:
                if runtime_req["outputDirType"] == "local_output_dir":
                    # Currently the default behavior.
                    pass
                elif runtime_req["outputDirType"] == "keep_output_dir":
                    mounts[self.outdir]= {
                        "kind": "collection",
                        "writable": True
                    }

        partition_req, _ = self.get_requirement("http://arvados.org/cwl#PartitionRequirement")
        if partition_req:
            scheduling_parameters["partitions"] = aslist(partition_req["partition"])

        intermediate_output_req, _ = self.get_requirement("http://arvados.org/cwl#IntermediateOutput")
        if intermediate_output_req:
            self.output_ttl = intermediate_output_req["outputTTL"]
        else:
            self.output_ttl = self.arvrunner.intermediate_output_ttl

        if self.output_ttl < 0:
            raise WorkflowException("Invalid value %d for output_ttl, cannot be less than zero" % container_request["output_ttl"])


        if self.arvrunner.api._rootDesc["revision"] >= "20210628":
            storage_class_req, _ = self.get_requirement("http://arvados.org/cwl#OutputStorageClass")
            if storage_class_req and storage_class_req.get("intermediateStorageClass"):
                container_request["output_storage_classes"] = aslist(storage_class_req["intermediateStorageClass"])
            else:
                container_request["output_storage_classes"] = (
                    runtimeContext.intermediate_storage_classes
                    or list(arvados.util.iter_storage_classes(self.arvrunner.api.config()))
                )

        cuda_req, _ = self.get_requirement("http://commonwl.org/cwltool#CUDARequirement")
        if cuda_req:
            if self.arvrunner.api._rootDesc["revision"] >= "20250128":
                # Arvados 3.1+ API
                runtime_constraints["gpu"] = {
                    "stack": "cuda",
                    "device_count": resources.get("cudaDeviceCount", 1),
                    "driver_version": cuda_req["cudaVersionMin"],
                    "hardware_target": aslist(cuda_req["cudaComputeCapability"]),
                    "vram": self.builder.do_eval(cuda_req.get("cudaVram", 0))*1024*1024,
                }
            else:
                # Legacy API
                runtime_constraints["cuda"] = {
                    "device_count": resources.get("cudaDeviceCount", 1),
                    "driver_version": cuda_req["cudaVersionMin"],
                    "hardware_capability": aslist(cuda_req["cudaComputeCapability"])[0]
                }

        rocm_req, _ = self.get_requirement("http://arvados.org/cwl#ROCmRequirement")
        if rocm_req:
            if self.arvrunner.api._rootDesc["revision"] >= "20250128":
                runtime_constraints["gpu"] = {
                    "stack": "rocm",
                    "device_count": self.builder.do_eval(rocm_req.get("rocmDeviceCountMin", None)) or self.builder.do_eval(rocm_req.get("rocmDeviceCountMax", 1)),
                    "driver_version": rocm_req["rocmDriverVersion"],
                    "hardware_target": aslist(rocm_req["rocmTarget"]),
                    "vram": self.builder.do_eval(rocm_req["rocmVram"])*1024*1024,
                }
            else:
                raise WorkflowException("Arvados API server does not support ROCm (requires Arvados 3.1+)")

        if runtimeContext.enable_preemptible is False:
            scheduling_parameters["preemptible"] = False
        else:
            preemptible_req, _ = self.get_requirement("http://arvados.org/cwl#UsePreemptible")
            if preemptible_req:
                scheduling_parameters["preemptible"] = preemptible_req["usePreemptible"]
            elif runtimeContext.enable_preemptible is True:
                scheduling_parameters["preemptible"] = True
            elif runtimeContext.enable_preemptible is None:
                pass

        if scheduling_parameters.get("preemptible") and self.may_resubmit_non_preemptible():
            # Only make one attempt, because if it is preempted we
            # will resubmit and ask for a non-preemptible instance.
            container_request["container_count_max"] = 1

        if self.timelimit is not None and self.timelimit > 0:
            scheduling_parameters["max_run_time"] = self.timelimit

        extra_submit_params = {}
        if runtimeContext.submit_runner_cluster:
            extra_submit_params["cluster_id"] = runtimeContext.submit_runner_cluster

        container_request["output_name"] = cleanup_name_for_collection("Output from step %s" % (self.name))
        container_request["output_ttl"] = self.output_ttl
        container_request["mounts"] = mounts
        container_request["secret_mounts"] = secret_mounts
        container_request["runtime_constraints"] = runtime_constraints
        container_request["scheduling_parameters"] = scheduling_parameters

        enable_reuse = runtimeContext.enable_reuse
        if enable_reuse:
            reuse_req, _ = self.get_requirement("WorkReuse")
            if reuse_req:
                enable_reuse = reuse_req["enableReuse"]
            reuse_req, _ = self.get_requirement("http://arvados.org/cwl#ReuseRequirement")
            if reuse_req:
                enable_reuse = reuse_req["enableReuse"]
        container_request["use_existing"] = enable_reuse

        properties_req, _ = self.get_requirement("http://arvados.org/cwl#ProcessProperties")
        if properties_req:
            for pr in properties_req["processProperties"]:
                container_request["properties"][pr["propertyName"]] = self.builder.do_eval(pr["propertyValue"])

        output_properties_req, _ = self.get_requirement("http://arvados.org/cwl#OutputCollectionProperties")
        if output_properties_req:
            if self.arvrunner.api._rootDesc["revision"] >= "20220510":
                container_request["output_properties"] = {}
                for pr in output_properties_req["outputProperties"]:
                    container_request["output_properties"][pr["propertyName"]] = self.builder.do_eval(pr["propertyValue"])
            else:
                logger.warning("%s API revision is %s, revision %s is required to support setting properties on output collections.",
                               self.arvrunner.label(self), self.arvrunner.api._rootDesc["revision"], "20220510")

        if self.arvrunner.api._rootDesc["revision"] >= "20240502" and self.globpatterns:
            output_glob = []
            try:
                for gp in self.globpatterns:
                    pattern = ""
                    gb = None
                    if isinstance(gp, str):
                        try:
                            gb = self.builder.do_eval(gp)
                        except:
                            raise OutputGlobError("Expression evaluation failed")
                    elif isinstance(gp, dict):
                        # dict of two keys, 'glob' and 'pattern' which
                        # means we should try to predict the names of
                        # secondary files to capture.
                        try:
                            gb = self.builder.do_eval(gp["glob"])
                        except:
                            raise OutputGlobError("Expression evaluation failed")
                        pattern = gp["pattern"]

                        if "${" in pattern or "$(" in pattern:
                            # pattern is an expression, need to evaluate
                            # it first.
                            if '*' in gb or "]" in gb:
                                # glob has wildcards, so we can't
                                # predict the secondary file name.
                                # Capture everything.
                                raise OutputGlobError("glob has wildcards, cannot predict secondary file name")

                            # After evealuating 'glob' we have a
                            # expected name we can provide to the
                            # expression.
                            nr, ne = os.path.splitext(gb)
                            try:
                                pattern = self.builder.do_eval(pattern, context={
                                    "path": gb,
                                    "basename": os.path.basename(gb),
                                    "nameext": ne,
                                    "nameroot": nr,
                                })
                            except:
                                raise OutputGlobError("Expression evaluation failed")
                            if isinstance(pattern, str):
                                # If we get a string back, that's the expected
                                # file name for the secondary file.
                                gb = pattern
                                pattern = ""
                            else:
                                # However, it is legal for this to return a
                                # file object or an array.  In that case we'll
                                # just capture everything.
                                raise OutputGlobError("secondary file expression did not evaluate to a string")
                    else:
                        # Should never happen, globpatterns is
                        # constructed in arvtool from data that has
                        # already gone through schema validation, but
                        # still good to have a fallback.
                        raise TypeError("Expected glob pattern to be a str or dict, was %s" % gp)

                    if not gb:
                        continue

                    for gbeval in aslist(gb):
                        if gbeval.startswith(self.outdir+"/"):
                            gbeval = gbeval[len(self.outdir)+1:]
                        while gbeval.startswith("./"):
                            gbeval = gbeval[2:]

                        if pattern:
                            # pattern is not an expression or we would
                            # have handled this earlier, so it must be
                            # a simple substitution on the secondary
                            # file name.
                            #
                            # 'pattern' was assigned in the earlier code block
                            #
                            # if there's a wild card in the glob, figure
                            # out if there's enough text after it that the
                            # suffix substitution can be done correctly.
                            cutpos = max(gbeval.find("*"), gbeval.find("]"))
                            if cutpos > -1:
                                tail = gbeval[cutpos+1:]
                                if tail.count(".") < pattern.count("^"):
                                    # the known suffix in the glob has
                                    # fewer dotted extensions than the
                                    # substition pattern wants to remove,
                                    # so we can't accurately predict
                                    # correct name glob in advance.
                                    gbeval = ""
                            if gbeval:
                                gbeval = substitute(gbeval, pattern)

                        if gbeval in (self.outdir, "", "."):
                            output_glob.append("**")
                        elif gbeval.endswith("/"):
                            output_glob.append(gbeval+"**")
                        else:
                            output_glob.append(gbeval)
                            output_glob.append(gbeval + "/**")

                if "**" in output_glob:
                    # if it's going to match all, prefer not to provide it
                    # at all.
                    output_glob.clear()
            except OutputGlobError as e:
                logger.debug("Unable to set a more specific output_glob (this is not an error): %s", e.args[0], exc_info=e)
                output_glob.clear()

            if output_glob:
                # Tools should either use cwl.output.json or
                # outputBinding globs. However, one CWL conformance
                # test has both, so we need to make sure we collect
                # cwl.output.json in this case. That test uses
                # cwl.output.json return a string, but also uses
                # outputBinding.
                output_glob.append("cwl.output.json")

                # It could happen that a tool creates cwl.output.json,
                # references a file, but also uses a outputBinding
                # glob that doesn't include the file being referenced.
                #
                # In this situation, output_glob will only match the
                # pattern we know about.  If cwl.output.json referred
                # to other files in the output, those would be
                # missing.  We could upload the entire output, but we
                # currently have no way of knowing at this point
                # whether cwl.output.json will be used this way.
                #
                # Because this is a corner case, I'm inclined to leave
                # this as a known issue for now.  No conformance tests
                # do this and I'd even be inclined to have it ruled
                # incompatible in the CWL spec if it did come up.
                # That said, in retrospect it would have been good to
                # require CommandLineTool to declare when it expects
                # cwl.output.json.

                container_request["output_glob"] = output_glob

        ram_multiplier = [1]

        oom_retry_req, _ = self.get_requirement("http://arvados.org/cwl#OutOfMemoryRetry")
        if oom_retry_req:
            if oom_retry_req.get('memoryRetryMultiplier'):
                ram_multiplier.append(oom_retry_req.get('memoryRetryMultiplier'))
            elif oom_retry_req.get('memoryRetryMultipler'):
                ram_multiplier.append(oom_retry_req.get('memoryRetryMultipler'))
            else:
                ram_multiplier.append(2)

        if runtimeContext.runnerjob.startswith("arvwf:"):
            wfuuid = runtimeContext.runnerjob[6:runtimeContext.runnerjob.index("#")]
            wfrecord = self.arvrunner.api.workflows().get(uuid=wfuuid).execute(num_retries=self.arvrunner.num_retries)
            if container_request["name"] == "main":
                container_request["name"] = wfrecord["name"]
            container_request["properties"]["template_uuid"] = wfuuid

        if self.attempt_count == 0:
            self.output_callback = self.arvrunner.get_wrapped_callback(self.output_callback)

        try:
            ram = runtime_constraints["ram"]

            self.uuid = runtimeContext.submit_request_uuid

            for i in ram_multiplier:
                runtime_constraints["ram"] = ram * i

                if self.uuid:
                    response = self.arvrunner.api.container_requests().update(
                        uuid=self.uuid,
                        body=container_request,
                        **extra_submit_params
                    ).execute(num_retries=self.arvrunner.num_retries)
                else:
                    response = self.arvrunner.api.container_requests().create(
                        body=container_request,
                        **extra_submit_params
                    ).execute(num_retries=self.arvrunner.num_retries)
                    self.uuid = response["uuid"]

                if response["container_uuid"] is not None:
                    break

            if response["container_uuid"] is None:
                runtime_constraints["ram"] = ram * ram_multiplier[self.attempt_count]

            container_request["state"] = "Committed"
            try:
                response = self.arvrunner.api.container_requests().update(
                    uuid=self.uuid,
                    body=container_request,
                    **extra_submit_params
                ).execute(num_retries=self.arvrunner.num_retries)
            except Exception as e:
                # If the request was actually processed but we didn't
                # receive a response, we'll re-try the request, but if
                # the container went directly from "Committed" to
                # "Final", the retry attempt will fail with a state
                # change error.  So if there's an error, double check
                # to see if the container is in the expected state.
                #
                # See discussion on #22160
                response = self.arvrunner.api.container_requests().get(
                    uuid=self.uuid
                ).execute(num_retries=self.arvrunner.num_retries)
                if response.get("state") not in ("Committed", "Final"):
                    raise

            self.arvrunner.process_submitted(self)
            self.attempt_count += 1

            if response["state"] == "Final":
                logger.info("%s reused container %s", self.arvrunner.label(self), response["container_uuid"])
            else:
                logger.info("%s %s state is %s", self.arvrunner.label(self), response["uuid"], response["state"])
        except Exception as e:
            logger.exception("%s error submitting container\n%s", self.arvrunner.label(self), e)
            logger.debug("Container request was %s", container_request)
            self.output_callback({}, "permanentFail")

    def may_resubmit_non_preemptible(self):
        if self.job_runtime.enable_resubmit_non_preemptible is False:
            # explicitly disabled
            return False

        spot_instance_retry_req, _ = self.get_requirement("http://arvados.org/cwl#PreemptionBehavior")
        if spot_instance_retry_req:
            if spot_instance_retry_req["resubmitNonPreemptible"] is False:
                # explicitly disabled by hint
                return False
        elif self.job_runtime.enable_resubmit_non_preemptible is None:
            # default behavior is we don't retry
            return False

        # At this point, by process of elimination either
        # resubmitNonPreemptible or enable_resubmit_non_preemptible
        # must be True, so now check if the container was actually
        # preempted.

        return True

    def spot_instance_retry(self, record, container):
        return self.may_resubmit_non_preemptible() and bool(container["runtime_status"].get("preemptionNotice"))

    def out_of_memory_retry(self, record, container):
        oom_retry_req, _ = self.get_requirement("http://arvados.org/cwl#OutOfMemoryRetry")
        if oom_retry_req is None:
            return False

        # Sometimes it gets killed with no warning
        if container["exit_code"] == 137:
            return True

        logc = arvados.collection.CollectionReader(record["log_uuid"],
                                                   api_client=self.arvrunner.api,
                                                   keep_client=self.arvrunner.keep_client,
                                                   num_retries=self.arvrunner.num_retries)

        loglines = [""]
        def callback(v1, v2, v3):
            loglines[0] = v3

        done.logtail(logc, callback, "", maxlen=1000)

        # Check allocation failure
        oom_matches = oom_retry_req.get('memoryErrorRegex') or r'(bad_alloc|out ?of ?memory|memory ?error|container using over 9.% of memory)'
        if re.search(oom_matches, loglines[0], re.IGNORECASE | re.MULTILINE):
            return True

        return False

    def done(self, record):
        outputs = {}
        retried = False
        rcode = None
        do_retry = False

        try:
            container = self.arvrunner.api.containers().get(
                uuid=record["container_uuid"]
            ).execute(num_retries=self.arvrunner.num_retries)

            if container["state"] == "Complete":
                rcode = container["exit_code"]
                if self.successCodes and rcode in self.successCodes:
                    processStatus = "success"
                elif self.temporaryFailCodes and rcode in self.temporaryFailCodes:
                    processStatus = "temporaryFail"
                elif self.permanentFailCodes and rcode in self.permanentFailCodes:
                    processStatus = "permanentFail"
                elif rcode == 0:
                    processStatus = "success"
                else:
                    processStatus = "permanentFail"

                if processStatus == "permanentFail" and self.attempt_count == 1 and self.out_of_memory_retry(record, container):
                    logger.info("%s Container failed with out of memory error.  Retrying container with more RAM.",
                                 self.arvrunner.label(self))
                    self.job_runtime = self.job_runtime.copy()
                    do_retry = True

                if rcode == 137 and not do_retry:
                    logger.warning("%s Container may have been killed for using too much RAM.  Try resubmitting with a higher 'ramMin' or use the arv:OutOfMemoryRetry feature.",
                                 self.arvrunner.label(self))
            else:
                processStatus = "permanentFail"

            if processStatus == "permanentFail" and self.attempt_count == 1 and self.spot_instance_retry(record, container):
                logger.info("%s Container failed because the preemptible instance it was running on was reclaimed.  Retrying container on a non-preemptible instance.")
                self.job_runtime = self.job_runtime.copy()
                self.job_runtime.enable_preemptible = False
                do_retry = True

            if do_retry:
                # Add a property indicating that this container was resubmitted.
                updateproperties = record["properties"].copy()
                olduuid = self.uuid
                self.job_runtime.submit_request_uuid = None
                self.uuid = None
                self.run(None)
                # this flag suppresses calling the output callback, we only want to set this
                # when we're sure that the resubmission has happened without issue.
                retried = True
                # Add a property to the old container request indicating it
                # was retried
                updateproperties["arv:failed_container_resubmitted"] = self.uuid
                self.arvrunner.api.container_requests().update(uuid=olduuid,
                                                               body={"properties": updateproperties}).execute()
                return

            logc = None
            if record["log_uuid"]:
                logc = arvados.collection.Collection(record["log_uuid"],
                                                     api_client=self.arvrunner.api,
                                                     keep_client=self.arvrunner.keep_client,
                                                     num_retries=self.arvrunner.num_retries)

            if processStatus == "permanentFail" and logc is not None:
                label = self.arvrunner.label(self)
                done.logtail(
                    logc, logger.error,
                    "%s (%s) error log:" % (label, record["uuid"]), maxlen=40, include_crunchrun=(rcode is None or rcode > 127))

            if record["output_uuid"]:
                if self.arvrunner.trash_intermediate or self.arvrunner.intermediate_output_ttl:
                    # Compute the trash time to avoid requesting the collection record.
                    trash_at = ciso8601.parse_datetime_as_naive(record["modified_at"]) + datetime.timedelta(0, self.arvrunner.intermediate_output_ttl)
                    aftertime = " at %s" % trash_at.strftime("%Y-%m-%d %H:%M:%S UTC") if self.arvrunner.intermediate_output_ttl else ""
                    orpart = ", or" if self.arvrunner.trash_intermediate and self.arvrunner.intermediate_output_ttl else ""
                    oncomplete = " upon successful completion of the workflow" if self.arvrunner.trash_intermediate else ""
                    logger.info("%s Intermediate output %s (%s) will be trashed%s%s%s." % (
                        self.arvrunner.label(self), record["output_uuid"], container["output"], aftertime, orpart, oncomplete))
                self.arvrunner.add_intermediate_output(record["output_uuid"])

            if container["output"]:
                outputs = done.done_outputs(self, container, "/tmp", self.outdir, "/keep")

            properties = record["properties"].copy()
            properties["cwl_output"] = outputs
            self.arvrunner.api.container_requests().update(
                uuid=self.uuid,
                body={"container_request": {"properties": properties}}
            ).execute(num_retries=self.arvrunner.num_retries)

            if logc is not None and self.job_runtime.enable_usage_report is not False:
                try:
                    summarizer = crunchstat_summary.summarizer.ContainerRequestSummarizer(
                        record,
                        collection_object=logc,
                        label=self.name,
                        arv=self.arvrunner.api)
                    summarizer.run()
                    with logc.open("usage_report.html", "wt") as mr:
                        mr.write(summarizer.html_report())
                    logc.save()

                    # Post warnings about nodes that are under-utilized.
                    for rc in summarizer._recommend_gen(lambda x: x):
                        self.job_runtime.usage_report_notes.append(rc)

                except Exception as e:
                    logger.warning("%s unable to generate resource usage report",
                                 self.arvrunner.label(self),
                                 exc_info=(e if self.arvrunner.debug else False))

        except WorkflowException as e:
            # Only include a stack trace if in debug mode.
            # A stack trace may obfuscate more useful output about the workflow.
            logger.error("%s unable to collect output from %s:\n%s",
                         self.arvrunner.label(self), container["output"], e, exc_info=(e if self.arvrunner.debug else False))
            processStatus = "permanentFail"
        except Exception:
            logger.exception("%s while getting output object:", self.arvrunner.label(self))
            processStatus = "permanentFail"
        finally:
            if not retried:
                self.output_callback(outputs, processStatus)


class RunnerContainer(Runner):
    """Submit and manage a container that runs arvados-cwl-runner."""

    def arvados_job_spec(self, runtimeContext, git_info):
        """Create an Arvados container request for this workflow.

        The returned dict can be used to create a container passed as
        the +body+ argument to container_requests().create().
        """

        adjustDirObjs(self.job_order, trim_listing)
        visit_class(self.job_order, ("File", "Directory"), trim_anonymous_location)
        visit_class(self.job_order, ("File", "Directory"), remove_redundant_fields)

        secret_mounts = {}
        for param in sorted(self.job_order.keys()):
            if self.secret_store.has_secret(self.job_order[param]):
                mnt = "/secrets/s%d" % len(secret_mounts)
                secret_mounts[mnt] = {
                    "kind": "text",
                    "content": self.secret_store.retrieve(self.job_order[param])
                }
                self.job_order[param] = {"$include": mnt}

        environment = {}

        if self.arvrunner.botosession is not None and runtimeContext.defer_downloads and runtimeContext.aws_credential_capture:
            # There are deferred downloads from S3.  Save our credentials to secret
            # storage
            secret_mounts["/var/lib/cwl/.aws/config"] = {
                    "kind": "text",
                    "content": """[default]
region = {}
""".format(self.arvrunner.botosession.region_name)
            }
            environment["AWS_CONFIG_FILE"] = "/var/lib/cwl/.aws/config"

            creds = self.arvrunner.botosession.get_credentials()
            secret_mounts["/var/lib/cwl/.aws/credentials"] = {
                    "kind": "text",
                    "content": """[default]
aws_access_key_id = {}
aws_secret_access_key = {}
""".format(creds.access_key, creds.secret_key)
            }
            environment["AWS_SHARED_CREDENTIALS_FILE"] = "/var/lib/cwl/.aws/credentials"

        container_image = arvados_jobs_image(self.arvrunner, self.jobs_image, runtimeContext)

        workflow_runner_req, _ = self.embedded_tool.get_requirement("http://arvados.org/cwl#WorkflowRunnerResources")
        if workflow_runner_req and workflow_runner_req.get("acrContainerImage"):
            container_image = workflow_runner_req.get("acrContainerImage")

        container_req = {
            "name": self.name,
            "output_path": "/var/spool/cwl",
            "cwd": "/var/spool/cwl",
            "priority": self.priority,
            "state": "Committed",
            "container_image": container_image,
            "mounts": {
                "/var/lib/cwl/cwl.input.json": {
                    "kind": "json",
                    "content": self.job_order
                },
                "stdout": {
                    "kind": "file",
                    "path": "/var/spool/cwl/cwl.output.json"
                },
                "/var/spool/cwl": {
                    "kind": "collection",
                    "writable": True
                }
            },
            "secret_mounts": secret_mounts,
            "runtime_constraints": {
                "vcpus": math.ceil(self.submit_runner_cores),
                "ram": 1024*1024 * (math.ceil(self.submit_runner_ram) + math.ceil(self.collection_cache_size)),
                "API": True
            },
            "use_existing": self.reuse_runner,
            "properties": {},
            "environment": environment
        }

        if self.embedded_tool.tool.get("id", "").startswith("keep:"):
            sp = self.embedded_tool.tool["id"].split('/')
            workflowcollection = sp[0][5:]
            workflowname = "/".join(sp[1:])
            workflowpath = "/var/lib/cwl/workflow/%s" % workflowname
            container_req["mounts"]["/var/lib/cwl/workflow"] = {
                "kind": "collection",
                "portable_data_hash": "%s" % workflowcollection
            }
        elif self.embedded_tool.tool.get("id", "").startswith("arvwf:"):
            uuid, frg = urllib.parse.urldefrag(self.embedded_tool.tool["id"])
            workflowpath = "/var/lib/cwl/workflow.json#" + frg
            packedtxt = self.loadingContext.loader.fetch_text(uuid)
            yaml = ruamel.yaml.YAML(typ='safe', pure=True)
            packed = yaml.load(packedtxt)
            container_req["mounts"]["/var/lib/cwl/workflow.json"] = {
                "kind": "json",
                "content": packed
            }
            container_req["properties"]["template_uuid"] = self.embedded_tool.tool["id"][6:33]
        elif self.embedded_tool.tool.get("id", "").startswith("file:"):
            raise WorkflowException("Tool id '%s' is a local file but expected keep: or arvwf:" % self.embedded_tool.tool.get("id"))
        else:
            main = self.loadingContext.loader.idx["_:main"]
            if main.get("id") == "_:main":
                del main["id"]
            workflowpath = "/var/lib/cwl/workflow.json#main"
            container_req["mounts"]["/var/lib/cwl/workflow.json"] = {
                "kind": "json",
                "content": main
            }

        container_req["properties"].update({k.replace("http://arvados.org/cwl#", "arv:"): v for k, v in git_info.items()})

        properties_req, _ = self.embedded_tool.get_requirement("http://arvados.org/cwl#ProcessProperties")
        if properties_req:
            builder = make_builder(self.job_order, self.embedded_tool.hints, self.embedded_tool.requirements, runtimeContext, self.embedded_tool.metadata)
            for pr in properties_req["processProperties"]:
                container_req["properties"][pr["propertyName"]] = builder.do_eval(pr["propertyValue"])

        # --local means execute the workflow instead of submitting a container request
        # --api=containers means use the containers API
        # --no-log-timestamps means don't add timestamps (the logging infrastructure does this)
        # --disable-validate because we already validated so don't need to do it again
        # --eval-timeout is the timeout for javascript invocation
        # --parallel-task-count is the number of threads to use for job submission
        # --enable/disable-reuse sets desired job reuse
        # --collection-cache-size sets aside memory to store collections
        command = ["arvados-cwl-runner",
                   "--local",
                   "--api=containers",
                   "--no-log-timestamps",
                   "--disable-validate",
                   "--disable-color",
                   "--eval-timeout=%s" % self.arvrunner.eval_timeout,
                   "--thread-count=%s" % self.arvrunner.thread_count,
                   "--enable-reuse" if self.enable_reuse else "--disable-reuse",
                   "--collection-cache-size=%s" % self.collection_cache_size]

        if self.output_name:
            command.append("--output-name=" + self.output_name)
            container_req["output_name"] = self.output_name

        if self.output_tags:
            command.append("--output-tags=" + self.output_tags)

        if runtimeContext.debug:
            command.append("--debug")

        if runtimeContext.storage_classes:
            command.append("--storage-classes=" + ",".join(runtimeContext.storage_classes))

        if runtimeContext.intermediate_storage_classes:
            command.append("--intermediate-storage-classes=" + ",".join(runtimeContext.intermediate_storage_classes))

        if runtimeContext.on_error:
            command.append("--on-error=" + self.on_error)

        if runtimeContext.intermediate_output_ttl:
            command.append("--intermediate-output-ttl=%d" % runtimeContext.intermediate_output_ttl)

        if runtimeContext.trash_intermediate:
            command.append("--trash-intermediate")

        if runtimeContext.project_uuid:
            command.append("--project-uuid="+runtimeContext.project_uuid)

        if self.enable_dev:
            command.append("--enable-dev")

        if runtimeContext.enable_preemptible is True:
            command.append("--enable-preemptible")

        if runtimeContext.enable_preemptible is False:
            command.append("--disable-preemptible")

        if runtimeContext.varying_url_params:
            command.append("--varying-url-params=" + runtimeContext.varying_url_params)

        if runtimeContext.prefer_cached_downloads:
            command.append("--prefer-cached-downloads")

        if runtimeContext.enable_usage_report is True:
            command.append("--enable-usage-report")

        if runtimeContext.enable_usage_report is False:
            command.append("--disable-usage-report")

        if self.fast_parser:
            command.append("--fast-parser")

        if self.arvrunner.selected_credential is not None:
            command.append("--use-credential="+self.arvrunner.selected_credential["uuid"])

        if runtimeContext.s3_public_bucket is True:
            command.append("--s3-public-bucket")

        command.extend([workflowpath, "/var/lib/cwl/cwl.input.json"])

        container_req["command"] = command

        return container_req


    def run(self, runtimeContext):
        runtimeContext.keepprefix = "keep:"
        job_spec = self.arvados_job_spec(runtimeContext, self.git_info)
        if runtimeContext.project_uuid:
            job_spec["owner_uuid"] = runtimeContext.project_uuid

        extra_submit_params = {}
        if runtimeContext.submit_runner_cluster:
            extra_submit_params["cluster_id"] = runtimeContext.submit_runner_cluster

        if runtimeContext.submit_request_uuid:
            if "cluster_id" in extra_submit_params:
                # Doesn't make sense for "update" and actually fails
                del extra_submit_params["cluster_id"]
            response = self.arvrunner.api.container_requests().update(
                uuid=runtimeContext.submit_request_uuid,
                body=job_spec,
                **extra_submit_params
            ).execute(num_retries=self.arvrunner.num_retries)
        else:
            response = self.arvrunner.api.container_requests().create(
                body=job_spec,
                **extra_submit_params
            ).execute(num_retries=self.arvrunner.num_retries)

        self.uuid = response["uuid"]
        self.arvrunner.process_submitted(self)

        logger.info("%s submitted container_request %s", self.arvrunner.label(self), response["uuid"])

        workbench2 = self.arvrunner.api.config()["Services"]["Workbench2"]["ExternalURL"]
        if workbench2:
            url = "{}processes/{}".format(workbench2, response["uuid"])
            logger.info("Monitor workflow progress at %s", url)


    def done(self, record):
        try:
            container = self.arvrunner.api.containers().get(
                uuid=record["container_uuid"]
            ).execute(num_retries=self.arvrunner.num_retries)
            container["log"] = record["log_uuid"]
        except Exception:
            logger.exception("%s while getting runner container", self.arvrunner.label(self))
            self.arvrunner.output_callback({}, "permanentFail")
        else:
            super(RunnerContainer, self).done(container)
