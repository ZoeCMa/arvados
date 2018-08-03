// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import * as React from 'react';
import {
    StyleRulesCallback, WithStyles, withStyles, Card,
    CardHeader, IconButton, CardContent, Grid, Chip
} from '@material-ui/core';
import { connect } from 'react-redux';
import { RouteComponentProps } from 'react-router';
import { ArvadosTheme } from '../../common/custom-theme';
import { RootState } from '../../store/store';
import { MoreOptionsIcon, CollectionIcon, CopyIcon } from '../../components/icon/icon';
import { DetailsAttribute } from '../../components/details-attribute/details-attribute';
import { CollectionResource } from '../../models/collection';
import { CollectionPanelFiles } from '../../views-components/collection-panel-files/collection-panel-files';
import * as CopyToClipboard from 'react-copy-to-clipboard';

type CssRules = 'card' | 'iconHeader' | 'tag' | 'copyIcon';

const styles: StyleRulesCallback<CssRules> = (theme: ArvadosTheme) => ({
    card: {
        marginBottom: '20px'
    },
    iconHeader: {
        fontSize: '1.875rem',
        color: theme.customs.colors.yellow700
    },
    tag: {
        marginRight: theme.spacing.unit
    },
    copyIcon: {
        marginLeft: theme.spacing.unit,
        fontSize: '1.125rem',
        cursor: 'pointer'
    }
});

interface CollectionPanelDataProps {
    item: CollectionResource;
}

interface CollectionPanelActionProps {
    onItemRouteChange: (collectionId: string) => void;
    onContextMenu: (event: React.MouseEvent<HTMLElement>, item: CollectionResource) => void;
}

type CollectionPanelProps = CollectionPanelDataProps & CollectionPanelActionProps
                            & WithStyles<CssRules> & RouteComponentProps<{ id: string }>;


export const CollectionPanel = withStyles(styles)(
    connect((state: RootState) => ({ item: state.collectionPanel.item! }))(
        class extends React.Component<CollectionPanelProps> {

            render() {
                const { classes, item, onContextMenu } = this.props;
                return <div>
                        <Card className={classes.card}>
                            <CardHeader
                                avatar={ <CollectionIcon className={classes.iconHeader} /> }
                                action={
                                    <IconButton
                                        aria-label="More options"
                                        onClick={event => onContextMenu(event, item)}>
                                        <MoreOptionsIcon />
                                    </IconButton>
                                }
                                title={item && item.name }
                                subheader={item && item.description} />
                            <CardContent>
                                <Grid container direction="column">
                                    <Grid item xs={6}>
                                    <DetailsAttribute label='Collection UUID' value={item && item.uuid}>
                                        <CopyToClipboard text={item && item.uuid}>
                                            <CopyIcon className={classes.copyIcon} />
                                        </CopyToClipboard>
                                    </DetailsAttribute>
                                    <DetailsAttribute label='Content size' value='54 MB' />
                                    <DetailsAttribute label='Owner' value={item && item.ownerUuid} />
                                    </Grid>
                                </Grid>
                            </CardContent>
                        </Card>

                        <Card className={classes.card}>
                            <CardHeader title="Tags" />
                            <CardContent>
                                <Grid container direction="column">
                                    <Grid item xs={4}>
                                        <Chip label="Tag 1" className={classes.tag}/>
                                        <Chip label="Tag 2" className={classes.tag}/>
                                        <Chip label="Tag 3" className={classes.tag}/>
                                    </Grid>
                                </Grid>
                            </CardContent>
                        </Card>
                        <div className={classes.card}>
                            <CollectionPanelFiles/>
                        </div>
                    </div>;
            }

            componentWillReceiveProps({ match, item, onItemRouteChange }: CollectionPanelProps) {
                if (!item || match.params.id !== item.uuid) {
                    onItemRouteChange(match.params.id);
                }
            }

        }
    )
);
