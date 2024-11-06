// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { DataColumns } from 'components/data-table/data-column';
import {
    ResourceName,
    ProcessStatus as ResourceStatus,
    ResourceType,
    ResourceOwnerWithNameLink,
    ResourcePortableDataHash,
    ResourceFileSize,
    ResourceFileCount,
    ResourceUUID,
    ResourceContainerUuid,
    ContainerRunTime,
    ResourceOutputUuid,
    ResourceLogUuid,
    ResourceParentProcess,
    ResourceModifiedByUserUuid,
    ResourceVersion,
    ResourceCreatedAtDate,
    ResourceLastModifiedDate,
    ResourceTrashDate,
    ResourceDeleteDate,
    renderType,
    renderName,
} from 'views-components/data-explorer/renderers';
import { ProjectResource } from 'models/project';
import { createTree } from 'models/tree';
import { SortDirection } from 'components/data-table/data-column';
import { getInitialResourceTypeFilters, getInitialProcessStatusFilters } from 'store/resource-type-filters/resource-type-filters';

export enum SharedWithMePanelColumnNames {
    NAME = 'Name',
    STATUS = 'Status',
    TYPE = 'Type',
    OWNER = 'Owner',
    PORTABLE_DATA_HASH = 'Portable Data Hash',
    FILE_SIZE = 'File Size',
    FILE_COUNT = 'File Count',
    UUID = 'UUID',
    CONTAINER_UUID = 'Container UUID',
    RUNTIME = 'Runtime',
    OUTPUT_UUID = 'Output UUID',
    LOG_UUID = 'Log UUID',
    PARENT_PROCESS = 'Parent Process UUID',
    MODIFIED_BY_USER_UUID = 'Modified by User UUID',
    VERSION = 'Version',
    CREATED_AT = 'Date Created',
    LAST_MODIFIED = 'Last Modified',
    TRASH_AT = 'Trash at',
    DELETE_AT = 'Delete at',
}

export const sharedWithMePanelColumns: DataColumns<string, ProjectResource> = [
    {
        name: SharedWithMePanelColumnNames.NAME,
        selected: true,
        configurable: true,
        sort: { direction: SortDirection.NONE, field: 'name' },
        filters: createTree(),
        render: (resource) => renderName(resource as ProjectResource),
    },
    {
        name: SharedWithMePanelColumnNames.STATUS,
        selected: true,
        configurable: true,
        mutuallyExclusiveFilters: true,
        filters: getInitialProcessStatusFilters(),
        render: (uuid) => <ResourceStatus uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.TYPE,
        selected: true,
        configurable: true,
        filters: getInitialResourceTypeFilters(),
        render: (resource) => renderType(resource as ProjectResource),
    },
    {
        name: SharedWithMePanelColumnNames.OWNER,
        selected: true,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceOwnerWithNameLink uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.PORTABLE_DATA_HASH,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourcePortableDataHash uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.FILE_SIZE,
        selected: true,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceFileSize uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.FILE_COUNT,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceFileCount uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.UUID,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceUUID uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.CONTAINER_UUID,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceContainerUuid uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.RUNTIME,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ContainerRunTime uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.OUTPUT_UUID,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceOutputUuid uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.LOG_UUID,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceLogUuid uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.PARENT_PROCESS,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceParentProcess uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.MODIFIED_BY_USER_UUID,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceModifiedByUserUuid uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.VERSION,
        selected: false,
        configurable: true,
        filters: createTree(),
        render: (uuid) => <ResourceVersion uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.CREATED_AT,
        selected: false,
        configurable: true,
        sort: { direction: SortDirection.NONE, field: 'createdAt' },
        filters: createTree(),
        render: (uuid) => <ResourceCreatedAtDate uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.LAST_MODIFIED,
        selected: true,
        configurable: true,
        sort: { direction: SortDirection.DESC, field: 'modifiedAt' },
        filters: createTree(),
        render: (uuid) => <ResourceLastModifiedDate uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.TRASH_AT,
        selected: false,
        configurable: true,
        sort: { direction: SortDirection.NONE, field: 'trashAt' },
        filters: createTree(),
        render: (uuid) => <ResourceTrashDate uuid={uuid as string} />,
    },
    {
        name: SharedWithMePanelColumnNames.DELETE_AT,
        selected: false,
        configurable: true,
        sort: { direction: SortDirection.NONE, field: 'deleteAt' },
        filters: createTree(),
        render: (uuid) => <ResourceDeleteDate uuid={uuid as string} />,
    },
];
