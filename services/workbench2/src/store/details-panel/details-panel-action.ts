// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { unionize, ofType, UnionOf } from 'common/unionize';
import { RootState } from 'store/store';
import { Dispatch } from 'redux';
import { getResource } from 'store/resources/resources';
import { ServiceRepository } from 'services/services';
import { resourcesActions } from 'store/resources/resources-actions';
import { snackbarActions, SnackbarKind } from 'store/snackbar/snackbar-actions';
import { FilterBuilder } from 'services/api/filter-builder';
import { OrderBuilder } from 'services/api/order-builder';
import { CollectionResource } from 'models/collection';
import { extractUuidKind, ResourceKind } from 'models/resource';
import { setSelectedResourceUuid } from 'store/selected-resource/selected-resource-actions';
import { deselectAllOthers, selectOne } from 'store/multiselect/multiselect-actions';

export const SLIDE_TIMEOUT = 500;
export const CLOSE_DRAWER = 'CLOSE_DRAWER'

export const detailsPanelActions = unionize({
    TOGGLE_DETAILS_PANEL: ofType<{}>(),
    OPEN_DETAILS_PANEL: ofType<number>(),
    LOAD_DETAILS_PANEL: ofType<string>(),
});

export type DetailsPanelAction = UnionOf<typeof detailsPanelActions>;

export const loadDetailsPanel = (uuid: string) =>
    (dispatch: Dispatch, getState: () => RootState) => {
        if (getState().detailsPanel.isOpened) {
            switch(extractUuidKind(uuid)) {
                case ResourceKind.COLLECTION:
                    const c = getResource<CollectionResource>(uuid)(getState().resources);
                    dispatch<any>(refreshCollectionVersionsList(c!.currentVersionUuid));
                    break;
                default:
                    break;
            }
        }
        dispatch(detailsPanelActions.LOAD_DETAILS_PANEL(uuid));
    };

export const openDetailsPanel = (uuid?: string, tabNr: number = 0) =>
    (dispatch: Dispatch) => {
        dispatch(detailsPanelActions.OPEN_DETAILS_PANEL(tabNr));
        if (uuid !== undefined) {
            dispatch<any>(selectOne(uuid));
            dispatch<any>(deselectAllOthers(uuid));
            dispatch<any>(setSelectedResourceUuid(uuid));
            dispatch<any>(loadDetailsPanel(uuid));
        }
    };

export const refreshCollectionVersionsList = (uuid: string) =>
    (dispatch: Dispatch, getState: () => RootState, services: ServiceRepository) => {
        services.collectionService.list({
            filters: new FilterBuilder()
                .addEqual('current_version_uuid', uuid)
                .getFilters(),
            includeOldVersions: true,
            order: new OrderBuilder<CollectionResource>().addDesc("version").getOrder()
        }).then(versions => dispatch(resourcesActions.SET_RESOURCES(versions.items))
        ).catch(e => snackbarActions.OPEN_SNACKBAR({
            message: `Couldn't retrieve versions: ${e.errors[0]}`,
            hideDuration: 2000,
            kind: SnackbarKind.ERROR })
        );
    };

export const toggleDetailsPanel = (uuid: string) => (dispatch: Dispatch, getState: () => RootState) => {
    const { detailsPanel }= getState()
    const isTargetUuidNew = uuid !== detailsPanel.resourceUuid
    if(isTargetUuidNew && uuid !== CLOSE_DRAWER && detailsPanel.isOpened){
        dispatch<any>(loadDetailsPanel(uuid));
    } else {
        // because of material-ui issue resizing details panel breaks tabs.
        // triggering window resize event fixes that.
        setTimeout(() => {
            window.dispatchEvent(new Event('resize'));
        }, SLIDE_TIMEOUT);
        dispatch(detailsPanelActions.TOGGLE_DETAILS_PANEL());
        if (getState().detailsPanel.isOpened) {
            dispatch<any>(loadDetailsPanel(isTargetUuidNew ? uuid : detailsPanel.resourceUuid));
        }
    }
    };
