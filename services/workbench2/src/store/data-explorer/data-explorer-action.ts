// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { unionize, ofType, UnionOf } from "common/unionize";
import { DataColumns } from "components/data-table/data-column";
import { DataTableFetchMode } from "components/data-table/data-table";
import { DataTableFilters } from "components/data-table-filters/data-table-filters";
import { SnackbarKind, snackbarActions } from "store/snackbar/snackbar-actions";

export enum DataTableRequestState {
    IDLE,
    PENDING,
    NEED_REFRESH,
}

export const dataExplorerActions = unionize({
    CLEAR: ofType<{ id: string }>(),
    RESET_PAGINATION: ofType<{ id: string }>(),
    SET_LOADING_ITEMS_AVAILABLE: ofType<{ id: string, loadingItemsAvailable: boolean }>(),
    SET_ITEMS_AVAILABLE: ofType<{ id: string, itemsAvailable: number }>(),
    RESET_ITEMS_AVAILABLE: ofType<{ id: string }>(),
    REQUEST_ITEMS: ofType<{ id: string; criteriaChanged?: boolean, background?: boolean }>(),
    REQUEST_COUNT: ofType<{ id: string; criteriaChanged?: boolean, background?: boolean }>(),
    REQUEST_STATE: ofType<{ id: string; criteriaChanged?: boolean }>(),
    SET_FETCH_MODE: ofType<{ id: string; fetchMode: DataTableFetchMode }>(),
    SET_COLUMNS: ofType<{ id: string; columns: DataColumns<any, any> }>(),
    SET_FILTERS: ofType<{ id: string; columnName: string; filters: DataTableFilters }>(),
    SET_WORKING: ofType<{ id: string, working: boolean }>(),
    SET_ITEMS: ofType<{ id: string; items: any[]; page: number; rowsPerPage: number; itemsAvailable?: number }>(),
    APPEND_ITEMS: ofType<{ id: string; items: any[]; page: number; rowsPerPage: number; itemsAvailable?: number }>(),
    SET_PAGE: ofType<{ id: string; page: number }>(),
    SET_ROWS_PER_PAGE: ofType<{ id: string; rowsPerPage: number }>(),
    TOGGLE_COLUMN: ofType<{ id: string; columnName: string }>(),
    TOGGLE_SORT: ofType<{ id: string; columnName: string }>(),
    SET_EXPLORER_SEARCH_VALUE: ofType<{ id: string; searchValue: string }>(),
    RESET_EXPLORER_SEARCH_VALUE: ofType<{ id: string }>(),
    SET_REQUEST_STATE: ofType<{ id: string; requestState: DataTableRequestState }>(),
    SET_COUNT_REQUEST_STATE: ofType<{ id: string; countRequestState: DataTableRequestState }>(),
    SET_IS_NOT_FOUND: ofType<{ id: string; isNotFound: boolean }>(),
});

export type DataExplorerAction = UnionOf<typeof dataExplorerActions>;

export const bindDataExplorerActions = (id: string) => ({
    CLEAR: () => dataExplorerActions.CLEAR({ id }),
    RESET_PAGINATION: () => dataExplorerActions.RESET_PAGINATION({ id }),
    SET_LOADING_ITEMS_AVAILABLE: (loadingItemsAvailable: boolean) => dataExplorerActions.SET_LOADING_ITEMS_AVAILABLE({ id, loadingItemsAvailable }),
    SET_ITEMS_AVAILABLE: (itemsAvailable: number) => dataExplorerActions.SET_ITEMS_AVAILABLE({ id, itemsAvailable }),
    RESET_ITEMS_AVAILABLE: () => dataExplorerActions.RESET_ITEMS_AVAILABLE({ id }),
    REQUEST_ITEMS: (criteriaChanged?: boolean, background?: boolean) => dataExplorerActions.REQUEST_ITEMS({ id, criteriaChanged, background }),
    REQUEST_COUNT: (criteriaChanged?: boolean, background?: boolean) => dataExplorerActions.REQUEST_COUNT({ id, criteriaChanged, background }),
    SET_FETCH_MODE: (payload: { fetchMode: DataTableFetchMode }) => dataExplorerActions.SET_FETCH_MODE({ ...payload, id }),
    SET_COLUMNS: (payload: { columns: DataColumns<any, any> }) => dataExplorerActions.SET_COLUMNS({ ...payload, id }),
    SET_FILTERS: (payload: { columnName: string; filters: DataTableFilters }) => dataExplorerActions.SET_FILTERS({ ...payload, id }),
    SET_WORKING: (working: boolean) => dataExplorerActions.SET_WORKING({ id, working }),
    SET_ITEMS: (payload: { items: any[]; page: number; rowsPerPage: number; itemsAvailable?: number }) =>
        dataExplorerActions.SET_ITEMS({ ...payload, id }),
    APPEND_ITEMS: (payload: { items: any[]; page: number; rowsPerPage: number; itemsAvailable?: number }) =>
        dataExplorerActions.APPEND_ITEMS({ ...payload, id }),
    SET_PAGE: (payload: { page: number }) => dataExplorerActions.SET_PAGE({ ...payload, id }),
    SET_ROWS_PER_PAGE: (payload: { rowsPerPage: number }) => dataExplorerActions.SET_ROWS_PER_PAGE({ ...payload, id }),
    TOGGLE_COLUMN: (payload: { columnName: string }) => dataExplorerActions.TOGGLE_COLUMN({ ...payload, id }),
    TOGGLE_SORT: (payload: { columnName: string }) => dataExplorerActions.TOGGLE_SORT({ ...payload, id }),
    SET_EXPLORER_SEARCH_VALUE: (payload: { searchValue: string }) => dataExplorerActions.SET_EXPLORER_SEARCH_VALUE({ ...payload, id }),
    RESET_EXPLORER_SEARCH_VALUE: () => dataExplorerActions.RESET_EXPLORER_SEARCH_VALUE({ id }),
    SET_REQUEST_STATE: (payload: { requestState: DataTableRequestState }) => dataExplorerActions.SET_REQUEST_STATE({ ...payload, id }),
    SET_COUNT_REQUEST_STATE: (payload: { countRequestState: DataTableRequestState }) => dataExplorerActions.SET_COUNT_REQUEST_STATE({ ...payload, id }),
    SET_IS_NOT_FOUND: (payload: { isNotFound: boolean }) => dataExplorerActions.SET_IS_NOT_FOUND({ ...payload, id }),
});

export type BoundDataExplorerActions = ReturnType<typeof bindDataExplorerActions>;

export const couldNotFetchItemsAvailable = () =>
    snackbarActions.OPEN_SNACKBAR({
        message: "Could not fetch total items.",
        kind: SnackbarKind.ERROR,
    });
