// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { unionize, ofType, UnionOf } from '~/common/unionize';
import { Dispatch } from 'redux';

export const detailsPanelActions = unionize({
    TOGGLE_DETAILS_PANEL: ofType<{}>(),
    LOAD_DETAILS_PANEL: ofType<string>()
});

export type DetailsPanelAction = UnionOf<typeof detailsPanelActions>;

export const loadDetailsPanel = (uuid: string) => detailsPanelActions.LOAD_DETAILS_PANEL(uuid);

export const toggleDetailsPanel = () => (dispatch: Dispatch) => {
    // because of material-ui issue resizing details panel breaks tabs.
    // triggering window resize event fixes that.
    const detailsPanelAnimationDuration = 500;
    setTimeout(() => {
        window.dispatchEvent(new Event('resize'));
    }, detailsPanelAnimationDuration);
    dispatch(detailsPanelActions.TOGGLE_DETAILS_PANEL());
};
