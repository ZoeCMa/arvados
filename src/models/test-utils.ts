// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { GroupResource, GroupClass } from "./group";
import { Resource, ResourceKind } from "./resource";
import { ProjectResource } from "./project";

export const mockGroupResource = (data: Partial<GroupResource> = {}): GroupResource => ({
    createdAt: "",
    deleteAt: "",
    description: "",
    etag: "",
    groupClass: null,
    href: "",
    isTrashed: false,
    kind: ResourceKind.Group,
    modifiedAt: "",
    modifiedByClientUuid: "",
    modifiedByUserUuid: "",
    name: "",
    ownerUuid: "",
    properties: "",
    trashAt: "",
    uuid: "",
    writeableBy: [],
    ...data
});

export const mockProjectResource = (data: Partial<ProjectResource> = {}): ProjectResource =>
    mockGroupResource({ ...data, groupClass: GroupClass.Project }) as ProjectResource;

export const mockCommonResource = (data: Partial<Resource>): Resource => ({
    createdAt: "",
    etag: "",
    href: "",
    kind: "",
    modifiedAt: "",
    modifiedByClientUuid: "",
    modifiedByUserUuid: "",
    ownerUuid: "",
    uuid: "",
    ...data
});
