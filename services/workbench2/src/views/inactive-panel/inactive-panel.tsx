// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import React from 'react';
import { Dispatch } from 'redux';
import { connect } from 'react-redux';
import { Grid, Typography, Button } from '@mui/material';
import { CustomStyleRulesCallback } from 'common/custom-theme';
import { WithStyles } from '@mui/styles';
import withStyles from '@mui/styles/withStyles';
import { ArvadosTheme } from 'common/custom-theme';
import { navigateToLinkAccount } from 'store/navigation/navigation-action';
import { RootState } from 'store/store';
import { sanitizeHTML } from 'common/html-sanitize';

export type CssRules = 'root' | 'ontop' | 'title';

const styles: CustomStyleRulesCallback<CssRules> = (theme: ArvadosTheme) => ({
    root: {
        position: 'relative',
        backgroundColor: theme.palette.grey["200"],
        background: 'url("arvados-logo-big.png") no-repeat center center',
        backgroundBlendMode: 'soft-light',
    },
    ontop: {
        zIndex: 10
    },
    title: {
        marginBottom: theme.spacing(6),
        color: theme.palette.grey["800"]
    }
});

export interface InactivePanelActionProps {
    startLinking: () => void;
}

const mapDispatchToProps = (dispatch: Dispatch): InactivePanelActionProps => ({
    startLinking: () => {
        dispatch<any>(navigateToLinkAccount);
    }
});

const mapStateToProps = (state: RootState): InactivePanelStateProps => ({
    inactivePageText: state.auth.config.clusterConfig.Workbench.InactivePageHTML,
    loginCluster: state.auth.config.clusterConfig.Login.LoginCluster,
});

export interface InactivePanelStateProps {
    inactivePageText: string;
    loginCluster: string;
}

type InactivePanelProps = WithStyles<CssRules> & InactivePanelActionProps & InactivePanelStateProps;

export const InactivePanelRoot = ({ classes, startLinking, inactivePageText, loginCluster }: InactivePanelProps) =>{
    const isLoginClusterFederation = loginCluster === "";
    return <Grid container justifyContent="center" alignItems="center" direction="column" spacing={3}
        className={classes.root}
        style={{ marginTop: 56, height: "100%" }}>
        <Grid item>
            <Typography>
                <span dangerouslySetInnerHTML={{ __html: sanitizeHTML(inactivePageText) }} style={{ margin: "1em" }} />
            </Typography>
        </Grid>
        { !isLoginClusterFederation
        ? <><Grid item>
            <Typography align="center">
            If you would like to use this login to access another account click "Link Account".
            </Typography>
        </Grid>
        <Grid item>
            <Button className={classes.ontop} color="primary" variant="contained" onClick={() => startLinking()}>
                Link Account
            </Button>
        </Grid></>
        : <><Grid item>
            <Typography align="center">
                If you would like to use this login to access another account, please contact your administrator.
            </Typography>
        </Grid></> }
    </Grid >
};

export const InactivePanel = connect(mapStateToProps, mapDispatchToProps)(
    withStyles(styles)(InactivePanelRoot));
