// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import React from "react";
import { ListItemIcon, ListItemText, ListItem, Tooltip } from "@mui/material";
import { AddFavoriteIcon, RemoveFavoriteIcon } from "components/icon/icon";
import { connect } from "react-redux";
import { RootState } from "store/store";
import { FavoritesState } from "store/favorites/favorites-reducer";

const toolbarIconClass = {
    width: '1rem',
    marginLeft: '-0.5rem',
    marginTop: '0.25rem',
}

const mapStateToProps = (state: RootState): Pick<ToggleFavoriteActionProps, 'selectedResourceUuid' | 'contextMenuResourceUuid' | 'favorites'> => ({
    contextMenuResourceUuid: state.contextMenu.resource?.uuid || '',
    selectedResourceUuid: state.selectedResourceUuid,
    favorites: state.favorites,
});

type ToggleFavoriteActionProps = {
    isInToolbar: boolean,
    contextMenuResourceUuid: string,
    selectedResourceUuid?: string,
    favorites: FavoritesState,
    onClick: () => void
}

export const ToggleFavoriteAction = connect(mapStateToProps)((props: ToggleFavoriteActionProps) => {
    const faveResourceUuid = props.isInToolbar ? props.selectedResourceUuid : props.contextMenuResourceUuid;
    const isFavorite = faveResourceUuid !== undefined && props.favorites[faveResourceUuid] === true;

    return <Tooltip title={isFavorite ? "Remove from favorites" : "Add to favorites"}>
        <ListItem
            button
            onClick={props.onClick}>
            <ListItemIcon style={props.isInToolbar ? toolbarIconClass : {}}>
                {isFavorite
                    ? <RemoveFavoriteIcon />
                    : <AddFavoriteIcon />}
            </ListItemIcon>
            {!props.isInToolbar &&
                <ListItemText style={{ textDecoration: 'none' }}>
                    {isFavorite
                        ? <>Remove from favorites</>
                        : <>Add to favorites</>}
                </ListItemText>}
        </ListItem >
    </Tooltip>
});
