// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { Dispatch } from "redux";
import { RootState } from "store/store";
import { ServiceRepository } from "services/services";
import { snackbarActions, SnackbarKind } from "store/snackbar/snackbar-actions";
import { trashPanelActions } from "store/trash-panel/trash-panel-action";
import { activateSidePanelTreeItem, loadSidePanelTreeProjects, SidePanelTreeCategory } from "store/side-panel-tree/side-panel-tree-actions";
import { projectPanelDataActions } from "store/project-panel/project-panel-action-bind";
import { sharedWithMePanelActions } from "store/shared-with-me-panel/shared-with-me-panel-actions";
import { ResourceKind } from "models/resource";
import { navigateTo, navigateToTrash } from "store/navigation/navigation-action";
import { matchFavoritesRoute, matchProjectRoute, matchSharedWithMeRoute, matchTrashRoute } from "routes/routes";
import { ContextMenuActionNames } from "views-components/context-menu/context-menu-action-set";
import { addDisabledButton } from "store/multiselect/multiselect-actions";
import { showGroupedCommonResourceResultToasts, updateResources } from "store/resources/resources-actions";
import { GroupResource } from "models/group";
import { favoritePanelActions } from "store/favorite-panel/favorite-panel-action";
import { CommonResourceServiceError } from "services/common-service/common-resource-service";
import _ from "lodash";

export const toggleProjectTrashed =
    (uuid: string, ownerUuid: string, isTrashed: boolean, isMulti: boolean) =>
        async (dispatch: Dispatch, getState: () => RootState, services: ServiceRepository): Promise<any> => {
            let errorMessage = "";
            let successMessage = "";
            let toggledResource: GroupResource | undefined = undefined;
            dispatch<any>(addDisabledButton(ContextMenuActionNames.MOVE_TO_TRASH));
            try {
                if (isTrashed) {
                    errorMessage = "Could not restore project from trash";
                    successMessage = "Restored project from trash";
                    toggledResource = await services.groupsService.untrash(uuid);
                    if (toggledResource) {
                        // Resource store must be updated with trashed flag for favorites tree to hide trashed
                        await dispatch<any>(updateResources([toggledResource]));
                    }
                    dispatch<any>(isMulti || !toggledResource ? navigateToTrash : navigateTo(uuid));
                    dispatch<any>(activateSidePanelTreeItem(uuid));
                    dispatch<any>(loadSidePanelTreeProjects(SidePanelTreeCategory.FAVORITES));
                } else {
                    errorMessage = "Could not move project to trash";
                    successMessage = "Added project to trash";
                    toggledResource = await services.groupsService.trash(uuid);
                    if (toggledResource) {
                        // Resource store must be updated with trashed flag for favorites tree to hide trashed
                        await dispatch<any>(updateResources([toggledResource]));
                    }
                    // Refresh favorites tree after trash/untrash
                    // must be await to avoid race conditions with the next loadSidePanelTreeProjects
                    await dispatch<any>(loadSidePanelTreeProjects(SidePanelTreeCategory.FAVORITES));
                    dispatch<any>(loadSidePanelTreeProjects(ownerUuid));

                    const { location } = getState().router;
                    if (matchSharedWithMeRoute(location ? location.pathname : "")) {
                        dispatch(sharedWithMePanelActions.REQUEST_ITEMS());
                    }
                    else {
                        dispatch<any>(navigateTo(ownerUuid));
                    }
                }
                if (toggledResource) {
                        dispatch(
                        snackbarActions.OPEN_SNACKBAR({
                            message: successMessage,
                            hideDuration: 2000,
                            kind: SnackbarKind.SUCCESS,
                        })
                    );
                }
            } catch (e) {
                if (e.status === 422) {
                    dispatch(
                        snackbarActions.OPEN_SNACKBAR({
                            message: "Could not restore project from trash: Duplicate name at destination",
                            kind: SnackbarKind.ERROR,
                        })
                    );
                } else {
                    dispatch(
                        snackbarActions.OPEN_SNACKBAR({
                            message: errorMessage,
                            kind: SnackbarKind.ERROR,
                        })
                    );
                }
            }
        };

/**
 * Toggles the trash status of an array of UUIDS based on the current isTrashed status
 * @param uuids list of uuids to trash/untrash
 * @param isTrashed Current trashed status to be toggled
 * @returns Dispatchable action that yields a void promise
 */
export const toggleCollectionTrashed =
    (uuids: string[], isTrashed: boolean) =>
        async (dispatch: Dispatch, getState: () => RootState, services: ServiceRepository): Promise<any> => {
            dispatch<any>(addDisabledButton(ContextMenuActionNames.MOVE_TO_TRASH));

            const verb = isTrashed ? "untrash" : "trash";
            const messageFuncMap = {
                [CommonResourceServiceError.NONE]: (count: number) => count > 1 ? `${_.startCase(verb)}ed ${count} items` : `Item ${verb}ed`,
                [CommonResourceServiceError.PERMISSION_ERROR_FORBIDDEN]: (count: number) => count > 1 ? `${_.startCase(verb)} ${count} items failed: Access Denied` : `${_.startCase(verb)} failed: Access Denied`,
                [CommonResourceServiceError.UNIQUE_NAME_VIOLATION]: (count: number) => count > 1 ? `${_.startCase(verb)} ${count} items failed: Duplicate Name` : `${_.startCase(verb)} failed: Duplicate Name`,
                [CommonResourceServiceError.UNKNOWN]: (count: number) => count > 1 ? `${_.startCase(verb)} ${count} items failed` : `${_.startCase(verb)} failed`,
            };

            await Promise.allSettled(uuids.map((uuid) => isTrashed ? services.collectionService.untrash(uuid) : services.collectionService.trash(uuid)))
                .then(async settledPromises => {
                    const { success } = showGroupedCommonResourceResultToasts(dispatch, settledPromises, messageFuncMap);

                    if (success.length) {
                        const { location } = getState().router;
                        // Update store
                        await dispatch<any>(updateResources(success.map(success => success.value)));
                        if (isTrashed) {
                            // Refresh trash panel after untrash
                            if (matchTrashRoute(location ? location.pathname : "")) {
                                dispatch(trashPanelActions.REQUEST_ITEMS());
                            }
                        } else {
                            // Refresh favorites / project view after trashed
                            if (matchFavoritesRoute(location ? location.pathname : "")) {
                                dispatch(favoritePanelActions.REQUEST_ITEMS());
                            } else if (matchProjectRoute(location ? location.pathname : "")) {
                                dispatch(projectPanelDataActions.REQUEST_ITEMS());
                            }
                        }
                        // Reload favorites
                        dispatch<any>(loadSidePanelTreeProjects(SidePanelTreeCategory.FAVORITES));
                    }
                });
        };

export const toggleTrashed = (kind: ResourceKind, uuid: string, ownerUuid: string, isTrashed: boolean) => (dispatch: Dispatch) => {
    if (kind === ResourceKind.PROJECT) {
        dispatch<any>(toggleProjectTrashed(uuid, ownerUuid, isTrashed!!, false));
    } else if (kind === ResourceKind.COLLECTION) {
        dispatch<any>(toggleCollectionTrashed([uuid], isTrashed!!));
    }
};
