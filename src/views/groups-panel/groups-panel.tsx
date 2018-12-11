// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import * as React from 'react';
import { connect } from 'react-redux';
import { Grid, Button } from "@material-ui/core";

import { DataExplorer } from "~/views-components/data-explorer/data-explorer";
import { DataColumns } from '~/components/data-table/data-table';
import { SortDirection } from '~/components/data-table/data-column';
import { ResourceOwner } from '~/views-components/data-explorer/renderers';
import { AddIcon } from '~/components/icon/icon';
import { ResourceName } from '~/views-components/data-explorer/renderers';
import { createTree } from '~/models/tree';
import { GROUPS_PANEL_ID, openCreateGroupDialog } from '~/store/groups-panel/groups-panel-actions';
import { noop } from 'lodash/fp';
import { ContextMenuKind } from '~/views-components/context-menu/context-menu';
import { getResource, ResourcesState } from '~/store/resources/resources';
import { GroupResource } from '~/models/group';
import { RootState } from '~/store/store';
import { Dispatch } from 'redux';
import { openContextMenu } from '~/store/context-menu/context-menu-actions';

export enum GroupsPanelColumnNames {
    GROUP = "Name",
    OWNER = "Owner",
    MEMBERS = "Members",
}

export const groupsPanelColumns: DataColumns<string> = [
    {
        name: GroupsPanelColumnNames.GROUP,
        selected: true,
        configurable: true,
        sortDirection: SortDirection.ASC,
        filters: createTree(),
        render: uuid => <ResourceName uuid={uuid} />
    },
    {
        name: GroupsPanelColumnNames.OWNER,
        selected: true,
        configurable: true,
        filters: createTree(),
        render: uuid => <ResourceOwner uuid={uuid} />,
    },
    {
        name: GroupsPanelColumnNames.MEMBERS,
        selected: true,
        configurable: true,
        filters: createTree(),
        render: uuid => <span>0</span>,
    },
];

const mapStateToProps = (state: RootState) => {
    return {
        resources: state.resources
    };
};

const mapDispatchToProps = (dispatch: Dispatch) => ({
    onContextMenu: (event: React.MouseEvent<HTMLElement>, item: any) => dispatch<any>(openContextMenu(event, item)),
    onNewGroup: openCreateGroupDialog
});

export interface GroupsPanelProps {
    onNewGroup: () => void;
    onContextMenu: (event: React.MouseEvent<HTMLElement>, item: any) => void;
    resources: ResourcesState;
}

export const GroupsPanel = connect(
    mapStateToProps, mapDispatchToProps
)(
    class GroupsPanel extends React.Component<GroupsPanelProps> {

        render() {
            return (
                <DataExplorer
                    id={GROUPS_PANEL_ID}
                    onRowClick={noop}
                    onRowDoubleClick={noop}
                    onContextMenu={this.handleContextMenu}
                    contextMenuColumn={true}
                    hideColumnSelector
                    actions={
                        <Grid container justify='flex-end'>
                            <Button
                                variant="contained"
                                color="primary"
                                onClick={this.props.onNewGroup}>
                                <AddIcon /> New group
                        </Button>
                        </Grid>
                    } />
            );
        }
        
        handleContextMenu = (event: React.MouseEvent<HTMLElement>, resourceUuid: string) => {
            const resource = getResource<GroupResource>(resourceUuid)(this.props.resources);
            if (resource) {
                this.props.onContextMenu(event, {
                    name: '',
                    uuid: resource.uuid,
                    ownerUuid: resource.ownerUuid,
                    kind: resource.kind,
                    menuKind: ContextMenuKind.GROUPS
                });
            }
        }
    });
