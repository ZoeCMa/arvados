// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { LinkService } from "../link-service/link-service";
import { GroupsService, GroupContentsResource } from "../groups-service/groups-service";
import { LinkClass, hasCreateLinkProperties, NewFavoriteLink } from "models/link";
import { FilterBuilder, joinFilters } from "services/api/filter-builder";
import { ListResults } from 'services/common-service/common-service';

export interface FavoriteListArguments {
    limit?: number;
    offset?: number;
    filters?: string;
    linkOrder?: string;
    contentOrder?: string;
}

export class FavoriteService {
    constructor(
        private linkService: LinkService,
        private groupsService: GroupsService,
    ) { }

    create(data: { userUuid: string; resource: { uuid: string; name: string } }) {
        const newLink: NewFavoriteLink = {
            ownerUuid: data.userUuid,
            tailUuid: data.userUuid,
            headUuid: data.resource.uuid,
            linkClass: LinkClass.STAR,
            name: data.resource.name
        }
        if (!hasCreateLinkProperties(newLink)) {
            return Promise.reject("Unable to create favorite: missing link properties");
        }
        return this.linkService.create(newLink);
    }

    delete(data: { userUuid: string; resourceUuid: string; }) {
        return this.linkService
            .list({
                filters: new FilterBuilder()
                    .addEqual('owner_uuid', data.userUuid)
                    .addEqual('head_uuid', data.resourceUuid)
                    .addEqual('link_class', LinkClass.STAR)
                    .getFilters()
            })
            .then(results => Promise.all(
                results.items.map(item => this.linkService.delete(item.uuid))));
    }

    list(userUuid: string, { filters, limit, offset, linkOrder, contentOrder }: FavoriteListArguments = {}, showOnlyOwned: boolean = true): Promise<ListResults<GroupContentsResource>> {
        const listFilters = new FilterBuilder()
            .addEqual('owner_uuid', userUuid)
            .addEqual('link_class', LinkClass.STAR)
            .getFilters();

        return this.linkService
            .list({
                filters: joinFilters(filters || '', listFilters),
                limit,
                offset,
                order: linkOrder
            })
            .then(results => {
                const uuids = results.items.map(item => item.headUuid);
                return this.groupsService.contents(showOnlyOwned ? userUuid : '', {
                    limit,
                    offset,
                    order: contentOrder,
                    filters: new FilterBuilder().addIn('uuid', uuids).getFilters(),
                    recursive: true
                });
            });
    }

    async checkPresenceInFavorites(userUuid: string, resourceUuids: string[]): Promise<Record<string, boolean>> {
        try {
            const result = await this.linkService
                .list({
                    filters: new FilterBuilder()
                        .addIn("head_uuid", resourceUuids)
                        .addEqual("owner_uuid", userUuid)
                        .addEqual("link_class", LinkClass.STAR)
                        .getFilters()
                })
                .then(( response ) => resourceUuids.reduce((results, uuid) => {
                    const filteredItems = response.items.filter(item => !!item.headUuid && item.linkClass === LinkClass.STAR);
                    const isFavorite = filteredItems.some(item => item.headUuid === uuid);
                    return { ...results, [uuid]: isFavorite };
                }, {}));
            return result;
        } catch (error) {
                console.error("Error while checking presence in favorites", error);
                return {};
        }
    }

}
