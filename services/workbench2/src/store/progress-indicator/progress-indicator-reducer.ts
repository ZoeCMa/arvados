// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import { ProgressIndicatorAction, progressIndicatorActions } from "store/progress-indicator/progress-indicator-actions";

type ProgressIndicatorState = string[];

const initialState: ProgressIndicatorState = [];

export const progressIndicatorReducer = (state: ProgressIndicatorState = initialState, action: ProgressIndicatorAction) => {
    return progressIndicatorActions.match(action, {
        START_WORKING: id => [...state, id],
        STOP_WORKING: id => state.filter(p => p !== id),
        default: () => state,
    });
};
