// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import * as React from 'react';
import IconBase, { IconTypes } from '../../icon/icon';
import Attribute from '../../attribute/attribute';
import AbstractItem from './abstract-item';
import { CollectionResource } from '../../../models/collection';
import { formatDate } from '../../../common/formatters';

export default class CollectionItem extends AbstractItem<CollectionResource> {
    
    constructor(item: CollectionResource) {
        super(item);
        console.log('Collection item: ', this.item);
    }

    getIcon(): IconTypes {
        return IconTypes.COLLECTION;
    }

    buildDetails(): React.ReactElement<any> {
        return <div>
           <Attribute label='Type' value='Data Collection' />
            <Attribute label='Size' value='---' />
            <Attribute label='Owner' value={this.item.ownerUuid} />
            <Attribute label='Last modified' value={formatDate(this.item.modifiedAt)} />
            <Attribute label='Created at' value={formatDate(this.item.createdAt)} />
            {/* Links but we dont have view */}
            <Attribute label='Collection UUID' link={this.item.uuid} value={this.item.uuid} />
            <Attribute label='Content address' link={this.item.portableDataHash} value={this.item.portableDataHash} />
            {/* Missing attrs */}
            <Attribute label='Number of files' value='20' />
            <Attribute label='Content size' value='54 MB' />
        </div>;
    }
}