# Copyright (C) The Arvados Authors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

# arv-copy [--recursive] [--no-recursive] object-uuid
#
# Copies an object from Arvados instance src to instance dst.
#
# By default, arv-copy recursively copies any dependent objects
# necessary to make the object functional in the new instance
# (e.g. for a workflow, arv-copy copies the workflow,
# input collections, and docker images). If
# --no-recursive is given, arv-copy copies only the single record
# identified by object-uuid.
#
# The user must have configuration files {src}.conf and
# {dst}.conf in a standard configuration directory with valid login credentials
# for instances src and dst.  If either of these files is not found,
# arv-copy will issue an error.

import argparse
import contextlib
import getpass
import os
import re
import shutil
import subprocess
import sys
import logging
import tempfile
import urllib.parse
import io
import json
import queue
import threading
import errno

import httplib2.error
import googleapiclient

import arvados
import arvados.config
import arvados.keep
import arvados.util
import arvados.commands._util as arv_cmd
import arvados.commands.keepdocker
from arvados.logging import log_handler

from arvados._internal import basedirs, http_to_keep, s3_to_keep, to_keep_util
from arvados._version import __version__

COMMIT_HASH_RE = re.compile(r'^[0-9a-f]{1,40}$')

arvlogger = logging.getLogger('arvados')
keeplogger = logging.getLogger('arvados.keep')
logger = logging.getLogger('arvados.arv-copy')

# Set this up so connection errors get logged.
googleapi_logger = logging.getLogger('googleapiclient.http')

# local_repo_dir records which git repositories from the Arvados source
# instance have been checked out locally during this run, and to which
# directories.
# e.g. if repository 'twp' from src_arv has been cloned into
# /tmp/gitfHkV9lu44A then local_repo_dir['twp'] = '/tmp/gitfHkV9lu44A'
#
local_repo_dir = {}

# List of collections that have been copied in this session, and their
# destination collection UUIDs.
collections_copied = {}

# Set of (repository, script_version) two-tuples of commits copied in git.
scripts_copied = set()

# The owner_uuid of the object being copied
src_owner_uuid = None

def main():
    copy_opts = argparse.ArgumentParser(add_help=False)

    copy_opts.add_argument(
        '--version', action='version', version="%s %s" % (sys.argv[0], __version__),
        help='Print version and exit.')
    copy_opts.add_argument(
        '-v', '--verbose', dest='verbose', action='store_true',
        help='Verbose output.')
    copy_opts.add_argument(
        '--progress', dest='progress', action='store_true',
        help='Report progress on copying collections. (default)')
    copy_opts.add_argument(
        '--no-progress', dest='progress', action='store_false',
        help='Do not report progress on copying collections.')
    copy_opts.add_argument(
        '-f', '--force', dest='force', action='store_true',
        help='Perform copy even if the object appears to exist at the remote destination.')
    copy_opts.add_argument(
        '--src', dest='source_arvados',
        help="""
Client configuration location for the source Arvados cluster.
May be either a configuration file path, or a plain identifier like `foo`
to search for a configuration file `foo.conf` under a systemd or XDG configuration directory.
If not provided, will search for a configuration file named after the cluster ID of the source object UUID.
""",
    )
    copy_opts.add_argument(
        '--dst', dest='destination_arvados',
        help="""
Client configuration location for the destination Arvados cluster.
May be either a configuration file path, or a plain identifier like `foo`
to search for a configuration file `foo.conf` under a systemd or XDG configuration directory.
If not provided, will use the default client configuration from the environment or `settings.conf`.
""",
    )
    copy_opts.add_argument(
        '--recursive', dest='recursive', action='store_true',
        help='Recursively copy any dependencies for this object, and subprojects. (default)')
    copy_opts.add_argument(
        '--no-recursive', dest='recursive', action='store_false',
        help='Do not copy any dependencies or subprojects.')
    copy_opts.add_argument(
        '--project-uuid', dest='project_uuid',
        help='The UUID of the project at the destination to which the collection or workflow should be copied.')
    copy_opts.add_argument(
        '--replication',
        type=arv_cmd.RangedValue(int, range(1, sys.maxsize)),
        metavar='N',
        help="""
Number of replicas per storage class for the copied collections at the destination.
If not provided (or if provided with invalid value),
use the destination's default replication-level setting (if found),
or the fallback value 2.
""")
    copy_opts.add_argument(
        '--storage-classes',
        type=arv_cmd.UniqueSplit(),
        help='Comma separated list of storage classes to be used when saving data to the destinaton Arvados instance.')
    copy_opts.add_argument("--varying-url-params", type=str, default="",
                        help="A comma separated list of URL query parameters that should be ignored when storing HTTP URLs in Keep.")

    copy_opts.add_argument("--prefer-cached-downloads", action="store_true", default=False,
                        help="If a HTTP URL is found in Keep, skip upstream URL freshness check (will not notice if the upstream has changed, but also not error if upstream is unavailable).")

    copy_opts.add_argument(
        'object_uuid',
        help='The UUID of the object to be copied.')
    copy_opts.set_defaults(progress=True)
    copy_opts.set_defaults(recursive=True)

    parser = argparse.ArgumentParser(
        description='Copy a workflow, collection or project from one Arvados instance to another.  On success, the uuid of the copied object is printed to stdout.',
        parents=[copy_opts, arv_cmd.retry_opt])
    args = parser.parse_args()

    if args.verbose:
        arvlogger.setLevel(logging.DEBUG)
    else:
        arvlogger.setLevel(logging.INFO)
        keeplogger.setLevel(logging.WARNING)

    if not args.source_arvados and arvados.util.uuid_pattern.match(args.object_uuid):
        args.source_arvados = args.object_uuid[:5]

    if not args.destination_arvados and args.project_uuid:
        args.destination_arvados = args.project_uuid[:5]

    # Make sure errors trying to connect to clusters get logged.
    googleapi_logger.setLevel(logging.WARN)
    googleapi_logger.addHandler(log_handler)

    # Create API clients for the source and destination instances
    src_arv = api_for_instance(args.source_arvados, args.retries)
    dst_arv = api_for_instance(args.destination_arvados, args.retries)

    # Once we've successfully contacted the clusters, we probably
    # don't want to see logging about retries (unless the user asked
    # for verbose output).
    if not args.verbose:
        googleapi_logger.setLevel(logging.ERROR)

    if src_arv.config()["ClusterID"] == dst_arv.config()["ClusterID"]:
        logger.info("Copying within cluster %s", src_arv.config()["ClusterID"])
    else:
        logger.info("Source cluster is %s", src_arv.config()["ClusterID"])
        logger.info("Destination cluster is %s", dst_arv.config()["ClusterID"])

    if not args.project_uuid:
        args.project_uuid = dst_arv.users().current().execute(num_retries=args.retries)["uuid"]

    # Identify the kind of object we have been given, and begin copying.
    t = uuid_type(src_arv, args.object_uuid)

    try:
        if t == 'Collection':
            set_src_owner_uuid(src_arv.collections(), args.object_uuid, args)
            result = copy_collection(args.object_uuid,
                                     src_arv, dst_arv,
                                     args)
        elif t == 'Workflow':
            set_src_owner_uuid(src_arv.workflows(), args.object_uuid, args)
            result = copy_workflow(args.object_uuid, src_arv, dst_arv, args)
        elif t == 'Group':
            set_src_owner_uuid(src_arv.groups(), args.object_uuid, args)
            result = copy_project(args.object_uuid, src_arv, dst_arv, args.project_uuid, args)
        elif t == 'httpURL' or t == 's3URL':
            result = copy_from_url(args.object_uuid, src_arv, dst_arv, args)
        else:
            abort("cannot copy object {} of type {}".format(args.object_uuid, t))
    except Exception as e:
        logger.error("%s", e, exc_info=args.verbose)
        exit(1)

    # Clean up any outstanding temp git repositories.
    for d in local_repo_dir.values():
        shutil.rmtree(d, ignore_errors=True)

    if not result:
        exit(1)

    # If no exception was thrown and the response does not have an
    # error_token field, presume success
    if result is None or 'error_token' in result or 'uuid' not in result:
        if result:
            logger.error("API server returned an error result: {}".format(result))
        exit(1)

    print(result['uuid'])

    if result.get('partial_error'):
        logger.warning("Warning: created copy with uuid {} but failed to copy some items: {}".format(result['uuid'], result['partial_error']))
        exit(1)

    logger.info("Success: created copy with uuid {}".format(result['uuid']))
    exit(0)

def set_src_owner_uuid(resource, uuid, args):
    global src_owner_uuid
    c = resource.get(uuid=uuid).execute(num_retries=args.retries)
    src_owner_uuid = c.get("owner_uuid")

# api_for_instance(instance_name)
#
#     Creates an API client for the Arvados instance identified by
#     instance_name.
#
#     If instance_name contains a slash, it is presumed to be a path
#     (either local or absolute) to a file with Arvados configuration
#     settings.
#
#     Otherwise, it is presumed to be the name of a file in a standard
#     configuration directory.
#
def api_for_instance(instance_name, num_retries):
    msg = []
    if instance_name:
        if '/' in instance_name:
            config_file = instance_name
        else:
            dirs = basedirs.BaseDirectories('CONFIG')
            config_file = next(dirs.search(f'{instance_name}.conf'), '')

        try:
            cfg = arvados.config.load(config_file)

            if 'ARVADOS_API_HOST' in cfg and 'ARVADOS_API_TOKEN' in cfg:
                api_is_insecure = (
                    cfg.get('ARVADOS_API_HOST_INSECURE', '').lower() in set(
                        ['1', 't', 'true', 'y', 'yes']))
                return arvados.api('v1',
                                     host=cfg['ARVADOS_API_HOST'],
                                     token=cfg['ARVADOS_API_TOKEN'],
                                     insecure=api_is_insecure,
                                     num_retries=num_retries,
                                     )
            else:
                msg.append('missing ARVADOS_API_HOST or ARVADOS_API_TOKEN for {} in config file {}'.format(instance_name, config_file))
        except OSError as e:
            if e.errno in (errno.EHOSTUNREACH, errno.ECONNREFUSED, errno.ECONNRESET, errno.ENETUNREACH):
                verb = 'connect to instance from'
            elif config_file:
                verb = 'open'
            else:
                verb = 'find'
                searchlist = ":".join(str(p) for p in dirs.search_paths())
                config_file = f'{instance_name}.conf in path {searchlist}'
            msg.append(("Could not {} config file {}: {}").format(
                       verb, config_file, e.strerror))
        except (httplib2.error.HttpLib2Error, googleapiclient.errors.Error) as e:
            msg.append("Failed to connect to instance {} at {}, error was {}".format(instance_name, cfg['ARVADOS_API_HOST'], e))

    default_api = None
    default_instance = None
    try:
        default_api = arvados.api('v1', num_retries=num_retries)
        default_instance = default_api.config()["ClusterID"]
    except ValueError:
        pass
    except (httplib2.error.HttpLib2Error, googleapiclient.errors.Error, OSError) as e:
        msg.append("Failed to connect to default instance, error was {}".format(e))

    if default_api is not None and (not instance_name or instance_name == default_instance):
        # Use default settings
        return default_api

    if instance_name and default_instance and instance_name != default_instance:
        msg.append("Default credentials are for {} but need to connect to {}".format(default_instance, instance_name))

    for m in msg:
        logger.error(m)

    abort('Unable to find usable ARVADOS_API_HOST and ARVADOS_API_TOKEN')

# Check if git is available
def check_git_availability():
    try:
        subprocess.run(
            ['git', '--version'],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        abort('git command is not available. Please ensure git is installed.')


def filter_iter(arg):
    """Iterate a filter string-or-list.

    Pass in a filter field that can either be a string or list.
    This will iterate elements as if the field had been written as a list.
    """
    if isinstance(arg, str):
        yield arg
    else:
        yield from arg

def migrate_repository_filter(repo_filter, src_repository, dst_repository):
    """Update a single repository filter in-place for the destination.

    If the filter checks that the repository is src_repository, it is
    updated to check that the repository is dst_repository.  If it does
    anything else, this function raises ValueError.
    """
    if src_repository is None:
        raise ValueError("component does not specify a source repository")
    elif dst_repository is None:
        raise ValueError("no destination repository specified to update repository filter")
    elif repo_filter[1:] == ['=', src_repository]:
        repo_filter[2] = dst_repository
    elif repo_filter[1:] == ['in', [src_repository]]:
        repo_filter[2] = [dst_repository]
    else:
        raise ValueError("repository filter is not a simple source match")

def migrate_script_version_filter(version_filter):
    """Update a single script_version filter in-place for the destination.

    Currently this function checks that all the filter operands are Git
    commit hashes.  If they're not, it raises ValueError to indicate that
    the filter is not portable.  It could be extended to make other
    transformations in the future.
    """
    if not all(COMMIT_HASH_RE.match(v) for v in filter_iter(version_filter[2])):
        raise ValueError("script_version filter is not limited to commit hashes")

def attr_filtered(filter_, *attr_names):
    """Return True if filter_ applies to any of attr_names, else False."""
    return any((name == 'any') or (name in attr_names)
               for name in filter_iter(filter_[0]))

@contextlib.contextmanager
def exception_handler(handler, *exc_types):
    """If any exc_types are raised in the block, call handler on the exception."""
    try:
        yield
    except exc_types as error:
        handler(error)


# copy_workflow(wf_uuid, src, dst, args)
#
#    Copies a workflow identified by wf_uuid from src to dst.
#
#    If args.recursive is True, also copy any collections
#      referenced in the workflow definition yaml.
#
#    The owner_uuid of the new workflow is set to any given
#      project_uuid or the user who copied the template.
#
#    Returns the copied workflow object.
#
def copy_workflow(wf_uuid, src, dst, args):
    # fetch the workflow from the source instance
    wf = src.workflows().get(uuid=wf_uuid).execute(num_retries=args.retries)

    if not wf["definition"]:
        logger.warning("Workflow object {} has an empty or null definition, it won't do anything.".format(wf_uuid))

    # copy collections and docker images
    if args.recursive and wf["definition"]:
        env = {"ARVADOS_API_HOST": urllib.parse.urlparse(src._rootDesc["rootUrl"]).netloc,
               "ARVADOS_API_TOKEN": src.api_token,
               "PATH": os.environ["PATH"]}
        try:
            result = subprocess.run(
                ["arvados-cwl-runner", "--quiet", "--print-keep-deps", "arvwf:"+wf_uuid],
                env=env,
                stdout=subprocess.PIPE,
                universal_newlines=True,
            )
        except FileNotFoundError:
            no_arv_copy = True
        else:
            no_arv_copy = result.returncode == 2

        if no_arv_copy:
            raise Exception('Copying workflows requires arvados-cwl-runner 2.7.1 or later to be installed in PATH.')
        elif result.returncode != 0:
            raise Exception('There was an error getting Keep dependencies from workflow using arvados-cwl-runner --print-keep-deps')

        locations = json.loads(result.stdout)

        if locations:
            copy_collections(locations, src, dst, args)

    # copy the workflow itself
    del wf['uuid']
    wf['owner_uuid'] = args.project_uuid

    existing = dst.workflows().list(filters=[["owner_uuid", "=", args.project_uuid],
                                             ["name", "=", wf["name"]]]).execute()
    if len(existing["items"]) == 0:
        return dst.workflows().create(body=wf).execute(num_retries=args.retries)
    else:
        return dst.workflows().update(uuid=existing["items"][0]["uuid"], body=wf).execute(num_retries=args.retries)


def workflow_collections(obj, locations, docker_images):
    if isinstance(obj, dict):
        loc = obj.get('location', None)
        if loc is not None:
            if loc.startswith("keep:"):
                locations.append(loc[5:])

        docker_image = obj.get('dockerImageId', None) or obj.get('dockerPull', None) or obj.get('acrContainerImage', None)
        if docker_image is not None:
            ds = docker_image.split(":", 1)
            tag = ds[1] if len(ds)==2 else 'latest'
            docker_images[ds[0]] = tag

        for x in obj:
            workflow_collections(obj[x], locations, docker_images)
    elif isinstance(obj, list):
        for x in obj:
            workflow_collections(x, locations, docker_images)

# copy_collections(obj, src, dst, args)
#
#    Recursively copies all collections referenced by 'obj' from src
#    to dst.  obj may be a dict or a list, in which case we run
#    copy_collections on every value it contains. If it is a string,
#    search it for any substring that matches a collection hash or uuid
#    (this will find hidden references to collections like
#      "input0": "$(file 3229739b505d2b878b62aed09895a55a+142/HWI-ST1027_129_D0THKACXX.1_1.fastq)")
#
#    Returns a copy of obj with any old collection uuids replaced by
#    the new ones.
#
def copy_collections(obj, src, dst, args):

    def copy_collection_fn(collection_match):
        """Helper function for regex substitution: copies a single collection,
        identified by the collection_match MatchObject, to the
        destination.  Returns the destination collection uuid (or the
        portable data hash if that's what src_id is).

        """
        src_id = collection_match.group(0)
        if src_id not in collections_copied:
            dst_col = copy_collection(src_id, src, dst, args)
            if src_id in [dst_col['uuid'], dst_col['portable_data_hash']]:
                collections_copied[src_id] = src_id
            else:
                collections_copied[src_id] = dst_col['uuid']
        return collections_copied[src_id]

    if isinstance(obj, str):
        # Copy any collections identified in this string to dst, replacing
        # them with the dst uuids as necessary.
        obj = arvados.util.portable_data_hash_pattern.sub(copy_collection_fn, obj)
        obj = arvados.util.collection_uuid_pattern.sub(copy_collection_fn, obj)
        return obj
    elif isinstance(obj, dict):
        return type(obj)((v, copy_collections(obj[v], src, dst, args))
                         for v in obj)
    elif isinstance(obj, list):
        return type(obj)(copy_collections(v, src, dst, args) for v in obj)
    return obj


def total_collection_size(manifest_text):
    """Return the total number of bytes in this collection (excluding
    duplicate blocks)."""

    total_bytes = 0
    locators_seen = {}
    for line in manifest_text.splitlines():
        words = line.split()
        for word in words[1:]:
            try:
                loc = arvados.KeepLocator(word)
            except ValueError:
                continue  # this word isn't a locator, skip it
            if loc.md5sum not in locators_seen:
                locators_seen[loc.md5sum] = True
                total_bytes += loc.size

    return total_bytes

def create_collection_from(c, src, dst, args):
    """Create a new collection record on dst, and copy Docker metadata if
    available."""

    collection_uuid = c['uuid']
    body = {}
    for d in ('description', 'manifest_text', 'name', 'portable_data_hash', 'properties'):
        body[d] = c[d]

    if not body["name"]:
        body['name'] = "copied from " + collection_uuid

    if args.storage_classes:
        body['storage_classes_desired'] = args.storage_classes

    body['owner_uuid'] = args.project_uuid

    dst_collection = dst.collections().create(body=body, ensure_unique_name=True).execute(num_retries=args.retries)

    # Create docker_image_repo+tag and docker_image_hash links
    # at the destination.
    for link_class in ("docker_image_repo+tag", "docker_image_hash"):
        docker_links = src.links().list(filters=[["head_uuid", "=", collection_uuid], ["link_class", "=", link_class]]).execute(num_retries=args.retries)['items']

        for src_link in docker_links:
            body = {key: src_link[key]
                    for key in ['link_class', 'name', 'properties']}
            body['head_uuid'] = dst_collection['uuid']
            body['owner_uuid'] = args.project_uuid

            lk = dst.links().create(body=body).execute(num_retries=args.retries)
            logger.debug('created dst link {}'.format(lk))

    return dst_collection

# copy_collection(obj_uuid, src, dst, args)
#
#    Copies the collection identified by obj_uuid from src to dst.
#    Returns the collection object created at dst.
#
#    If args.progress is True, produce a human-friendly progress
#    report.
#
#    If a collection with the desired portable_data_hash already
#    exists at dst, and args.force is False, copy_collection returns
#    the existing collection without copying any blocks.  Otherwise
#    (if no collection exists or if args.force is True)
#    copy_collection copies all of the collection data blocks from src
#    to dst.
#
#    For this application, it is critical to preserve the
#    collection's manifest hash, which is not guaranteed with the
#    arvados.CollectionReader and arvados.CollectionWriter classes.
#    Copying each block in the collection manually, followed by
#    the manifest block, ensures that the collection's manifest
#    hash will not change.
#
def copy_collection(obj_uuid, src, dst, args):
    if arvados.util.keep_locator_pattern.match(obj_uuid):
        # If the obj_uuid is a portable data hash, it might not be
        # uniquely identified with a particular collection.  As a
        # result, it is ambiguous as to what name to use for the copy.
        # Apply some heuristics to pick which collection to get the
        # name from.
        srccol = src.collections().list(
            filters=[['portable_data_hash', '=', obj_uuid]],
            order="created_at asc"
            ).execute(num_retries=args.retries)

        items = srccol.get("items")

        if not items:
            logger.warning("Could not find collection with portable data hash %s", obj_uuid)
            return

        c = None

        if len(items) == 1:
            # There's only one collection with the PDH, so use that.
            c = items[0]
        if not c:
            # See if there is a collection that's in the same project
            # as the root item (usually a workflow) being copied.
            for i in items:
                if i.get("owner_uuid") == src_owner_uuid and i.get("name"):
                    c = i
                    break
        if not c:
            # Didn't find any collections located in the same project, so
            # pick the oldest collection that has a name assigned to it.
            for i in items:
                if i.get("name"):
                    c = i
                    break
        if not c:
            # None of the collections have names (?!), so just pick the
            # first one.
            c = items[0]

        # list() doesn't return manifest text (and we don't want it to,
        # because we don't need the same maninfest text sent to us 50
        # times) so go and retrieve the collection object directly
        # which will include the manifest text.
        c = src.collections().get(uuid=c["uuid"]).execute(num_retries=args.retries)
    else:
        # Assume this is an actual collection uuid, so fetch it directly.
        c = src.collections().get(uuid=obj_uuid).execute(num_retries=args.retries)

    # If a collection with this hash already exists at the
    # destination, and 'force' is not true, just return that
    # collection.
    if not args.force:
        if 'portable_data_hash' in c:
            colhash = c['portable_data_hash']
        else:
            colhash = c['uuid']
        dstcol = dst.collections().list(
            filters=[['portable_data_hash', '=', colhash]]
        ).execute(num_retries=args.retries)
        if dstcol['items_available'] > 0:
            for d in dstcol['items']:
                if ((args.project_uuid == d['owner_uuid']) and
                    (c.get('name') == d['name']) and
                    (c['portable_data_hash'] == d['portable_data_hash'])):
                    return d
            c['manifest_text'] = dst.collections().get(
                uuid=dstcol['items'][0]['uuid']
            ).execute(num_retries=args.retries)['manifest_text']
            return create_collection_from(c, src, dst, args)

    if args.replication is None:
        # Obtain default or fallback collection replication setting on the
        # destination
        try:
            args.replication = int(dst.config()["Collections"]["DefaultReplication"])
        except (KeyError, TypeError, ValueError):
            args.replication = 2

    # Fetch the collection's manifest.
    manifest = c['manifest_text']
    logger.debug("Copying collection %s with manifest: <%s>", obj_uuid, manifest)

    # Copy each block from src_keep to dst_keep.
    # Use the newly signed locators returned from dst_keep to build
    # a new manifest as we go.
    src_keep = arvados.keep.KeepClient(api_client=src, num_retries=args.retries)
    dst_keep = arvados.keep.KeepClient(api_client=dst, num_retries=args.retries)
    dst_manifest = io.StringIO()
    dst_locators = {}
    bytes_written = 0
    bytes_expected = total_collection_size(manifest)
    if args.progress:
        progress_writer = ProgressWriter(human_progress)
    else:
        progress_writer = None

    # go through the words
    # put each block loc into 'get' queue
    # 'get' threads get block and put it into 'put' queue
    # 'put' threads put block and then update dst_locators
    #
    # after going through the whole manifest we go back through it
    # again and build dst_manifest

    lock = threading.Lock()

    # the get queue should be unbounded because we'll add all the
    # block hashes we want to get, but these are small
    get_queue = queue.Queue()

    threadcount = 4

    # the put queue contains full data blocks
    # and if 'get' is faster than 'put' we could end up consuming
    # a great deal of RAM if it isn't bounded.
    put_queue = queue.Queue(threadcount)
    transfer_error = []

    def get_thread():
        while True:
            word = get_queue.get()
            if word is None:
                put_queue.put(None)
                get_queue.task_done()
                return

            blockhash = arvados.KeepLocator(word).md5sum
            with lock:
                if blockhash in dst_locators:
                    # Already uploaded
                    get_queue.task_done()
                    continue

            try:
                logger.debug("Getting block %s", word)
                data = src_keep.get(word)
                put_queue.put((word, data))
            except Exception as e:
                logger.error("Error getting block %s: %s", word, e)
                transfer_error.append(e)
                try:
                    # Drain the 'get' queue so we end early
                    while True:
                        get_queue.get(False)
                        get_queue.task_done()
                except queue.Empty:
                    pass
            finally:
                get_queue.task_done()

    def put_thread():
        nonlocal bytes_written
        while True:
            item = put_queue.get()
            if item is None:
                put_queue.task_done()
                return

            word, data = item
            loc = arvados.KeepLocator(word)
            blockhash = loc.md5sum
            with lock:
                if blockhash in dst_locators:
                    # Already uploaded
                    put_queue.task_done()
                    continue

            try:
                logger.debug("Putting block %s (%s bytes)", blockhash, loc.size)
                dst_locator = dst_keep.put(data, copies=args.replication, classes=(args.storage_classes or []))
                with lock:
                    dst_locators[blockhash] = dst_locator
                    bytes_written += loc.size
                    if progress_writer:
                        progress_writer.report(obj_uuid, bytes_written, bytes_expected)
            except Exception as e:
                logger.error("Error putting block %s (%s bytes): %s", blockhash, loc.size, e)
                try:
                    # Drain the 'get' queue so we end early
                    while True:
                        get_queue.get(False)
                        get_queue.task_done()
                except queue.Empty:
                    pass
                transfer_error.append(e)
            finally:
                put_queue.task_done()

    for line in manifest.splitlines():
        words = line.split()
        for word in words[1:]:
            try:
                loc = arvados.KeepLocator(word)
            except ValueError:
                # If 'word' can't be parsed as a locator,
                # presume it's a filename.
                continue

            get_queue.put(word)

    for i in range(0, threadcount):
        get_queue.put(None)

    for i in range(0, threadcount):
        threading.Thread(target=get_thread, daemon=True).start()

    for i in range(0, threadcount):
        threading.Thread(target=put_thread, daemon=True).start()

    get_queue.join()
    put_queue.join()

    if len(transfer_error) > 0:
        return {"error_token": "Failed to transfer blocks"}

    for line in manifest.splitlines():
        words = line.split()
        dst_manifest.write(words[0])
        for word in words[1:]:
            try:
                loc = arvados.KeepLocator(word)
            except ValueError:
                # If 'word' can't be parsed as a locator,
                # presume it's a filename.
                dst_manifest.write(' ')
                dst_manifest.write(word)
                continue
            blockhash = loc.md5sum
            dst_manifest.write(' ')
            dst_manifest.write(dst_locators[blockhash])
        dst_manifest.write("\n")

    if progress_writer:
        progress_writer.report(obj_uuid, bytes_written, bytes_expected)
        progress_writer.finish()

    # Copy the manifest and save the collection.
    logger.debug('saving %s with manifest: <%s>', obj_uuid, dst_manifest.getvalue())

    c['manifest_text'] = dst_manifest.getvalue()
    return create_collection_from(c, src, dst, args)

def copy_docker_image(docker_image, docker_image_tag, src, dst, args):
    """Copy the docker image identified by docker_image and
    docker_image_tag from src to dst. Create appropriate
    docker_image_repo+tag and docker_image_hash links at dst.

    """

    logger.debug('copying docker image {}:{}'.format(docker_image, docker_image_tag))

    # Find the link identifying this docker image.
    docker_image_list = arvados.commands.keepdocker.list_images_in_arv(
        src, args.retries, docker_image, docker_image_tag)
    if docker_image_list:
        image_uuid, image_info = docker_image_list[0]
        logger.debug('copying collection {} {}'.format(image_uuid, image_info))

        # Copy the collection it refers to.
        dst_image_col = copy_collection(image_uuid, src, dst, args)
    elif arvados.util.keep_locator_pattern.match(docker_image):
        dst_image_col = copy_collection(docker_image, src, dst, args)
    else:
        logger.warning('Could not find docker image {}:{}'.format(docker_image, docker_image_tag))

def copy_project(obj_uuid, src, dst, owner_uuid, args):

    src_project_record = src.groups().get(uuid=obj_uuid).execute(num_retries=args.retries)

    # Create/update the destination project
    existing = dst.groups().list(filters=[["owner_uuid", "=", owner_uuid],
                                          ["name", "=", src_project_record["name"]]]).execute(num_retries=args.retries)
    if len(existing["items"]) == 0:
        project_record = dst.groups().create(body={"group": {"group_class": "project",
                                                             "owner_uuid": owner_uuid,
                                                             "name": src_project_record["name"]}}).execute(num_retries=args.retries)
    else:
        project_record = existing["items"][0]

    dst.groups().update(uuid=project_record["uuid"],
                        body={"group": {
                            "description": src_project_record["description"]}}).execute(num_retries=args.retries)

    args.project_uuid = project_record["uuid"]

    logger.debug('Copying %s to %s', obj_uuid, project_record["uuid"])


    partial_error = ""

    # Copy collections
    try:
        copy_collections([col["uuid"] for col in arvados.util.keyset_list_all(src.collections().list, filters=[["owner_uuid", "=", obj_uuid]])],
                         src, dst, args)
    except Exception as e:
        partial_error += "\n" + str(e)

    # Copy workflows
    for w in arvados.util.keyset_list_all(src.workflows().list, filters=[["owner_uuid", "=", obj_uuid]]):
        try:
            copy_workflow(w["uuid"], src, dst, args)
        except Exception as e:
            partial_error += "\n" + "Error while copying %s: %s" % (w["uuid"], e)

    if args.recursive:
        for g in arvados.util.keyset_list_all(src.groups().list, filters=[["owner_uuid", "=", obj_uuid]]):
            try:
                copy_project(g["uuid"], src, dst, project_record["uuid"], args)
            except Exception as e:
                partial_error += "\n" + "Error while copying %s: %s" % (g["uuid"], e)

    project_record["partial_error"] = partial_error

    return project_record

# git_rev_parse(rev, repo)
#
#    Returns the 40-character commit hash corresponding to 'rev' in
#    git repository 'repo' (which must be the path of a local git
#    repository)
#
def git_rev_parse(rev, repo):
    proc = subprocess.run(
        ['git', 'rev-parse', rev],
        check=True,
        cwd=repo,
        stdout=subprocess.PIPE,
        text=True,
    )
    return proc.stdout.read().strip()

# uuid_type(api, object_uuid)
#
#    Returns the name of the class that object_uuid belongs to, based on
#    the second field of the uuid.  This function consults the api's
#    schema to identify the object class.
#
#    It returns a string such as 'Collection', 'Workflow', etc.
#
#    Special case: if handed a Keep locator hash, return 'Collection'.
#
def uuid_type(api, object_uuid):
    if re.match(arvados.util.keep_locator_pattern, object_uuid):
        return 'Collection'

    if object_uuid.startswith("http:") or object_uuid.startswith("https:"):
        return 'httpURL'

    if object_uuid.startswith("s3:"):
        return 's3URL'

    p = object_uuid.split('-')
    if len(p) == 3:
        type_prefix = p[1]
        for k in api._schema.schemas:
            obj_class = api._schema.schemas[k].get('uuidPrefix', None)
            if type_prefix == obj_class:
                return k
    return None


def copy_from_url(url, src, dst, args):

    project_uuid = args.project_uuid
    # Ensure string of varying parameters is well-formed
    prefer_cached_downloads = args.prefer_cached_downloads

    cached = to_keep_util.CheckCacheResult(None, None, None, None, None)

    if url.startswith("http:") or url.startswith("https:"):
        cached = http_to_keep.check_cached_url(src, project_uuid, url, {},
                                               varying_url_params=args.varying_url_params,
                                               prefer_cached_downloads=prefer_cached_downloads)
    elif url.startswith("s3:"):
        import boto3.session
        botosession = boto3.session.Session()
        cached = s3_to_keep.check_cached_url(src, botosession, project_uuid, url, {},
                                             prefer_cached_downloads=prefer_cached_downloads)

    if cached[2] is not None:
        return copy_collection(cached[2], src, dst, args)

    if url.startswith("http:") or url.startswith("https:"):
        cached = http_to_keep.http_to_keep(dst, project_uuid, url,
                                           varying_url_params=args.varying_url_params,
                                           prefer_cached_downloads=prefer_cached_downloads)
    elif url.startswith("s3:"):
        cached = s3_to_keep.s3_to_keep(dst, botosession, project_uuid, url,
                                       prefer_cached_downloads=prefer_cached_downloads)

    if cached is not None:
        return {"uuid": cached[2]}


def abort(msg, code=1):
    logger.info("arv-copy: %s", msg)
    exit(code)


# Code for reporting on the progress of a collection upload.
# Stolen from arvados.commands.put.ArvPutCollectionWriter
# TODO(twp): figure out how to refactor into a shared library
# (may involve refactoring some arvados.commands.arv_copy.copy_collection
# code)

def machine_progress(obj_uuid, bytes_written, bytes_expected):
    return "{} {}: {} {} written {} total\n".format(
        sys.argv[0],
        os.getpid(),
        obj_uuid,
        bytes_written,
        -1 if (bytes_expected is None) else bytes_expected)

def human_progress(obj_uuid, bytes_written, bytes_expected):
    if bytes_expected:
        return "\r{}: {}M / {}M {:.1%} ".format(
            obj_uuid,
            bytes_written >> 20, bytes_expected >> 20,
            float(bytes_written) / bytes_expected)
    else:
        return "\r{}: {} ".format(obj_uuid, bytes_written)

class ProgressWriter(object):
    _progress_func = None
    outfile = sys.stderr

    def __init__(self, progress_func):
        self._progress_func = progress_func

    def report(self, obj_uuid, bytes_written, bytes_expected):
        if self._progress_func is not None:
            self.outfile.write(
                self._progress_func(obj_uuid, bytes_written, bytes_expected))

    def finish(self):
        self.outfile.write("\n")

if __name__ == '__main__':
    main()
