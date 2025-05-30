// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

import React from 'react';
import {
    Input as MuiInput,
    Chip as MuiChip,
    Popper as MuiPopper,
    Paper as MuiPaper,
    FormControl,
    InputLabel,
    ListItemText,
    ListItem,
    List,
    FormHelperText,
    Tooltip,
    Typography,
} from '@mui/material';
import withStyles from '@mui/styles/withStyles';
import { CustomStyleRulesCallback } from 'common/custom-theme';
import { PopperProps } from '@mui/material/Popper';
import { WithStyles } from '@mui/styles';
import { noop } from 'lodash';
import { isUserGroup } from 'models/group';
import { sortByKey } from 'common/objects';
import { TabbedList } from 'components/tabbedList/tabbed-list';
import maxSize from 'popper-max-size-modifier';
import { beforeWrite } from '@popperjs/core/lib';

export interface AutocompleteProps<Item, Suggestion> {
    label?: string;
    value: string;
    items: Item[];
    disabled?: boolean;
    suggestions?: Suggestion[];
    error?: boolean;
    helperText?: string;
    autofocus?: boolean;
    onChange: (event: React.ChangeEvent<HTMLInputElement>) => void;
    onBlur?: (event: React.FocusEvent<HTMLInputElement>) => void;
    onFocus?: (event: React.FocusEvent<HTMLInputElement>) => void;
    onCreate?: () => void;
    onDelete?: (item: Item, index: number) => void;
    onSelect?: (suggestion: Suggestion) => void;
    renderChipValue?: (item: Item) => string;
    renderChipTooltip?: (item: Item) => string;
    renderSuggestion?: (suggestion: Suggestion) => React.ReactNode;
    category?: AutocompleteCat;
    isWorking?: boolean;
    maxLength?: number;
}

type AutocompleteClasses = 'listItemStyle';

const autocompleteStyles: CustomStyleRulesCallback<AutocompleteClasses> = theme => ({
    listItemStyle: {
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
    },
});

export enum AutocompleteCat {
    SHARING = 'sharing',
};

export interface AutocompleteState {
    suggestionsOpen: boolean;
    selectedTab: number;
    selectedSuggestionIndex: number;
    tabbedListContents: Record<string, any[]>;
}

export const Autocomplete = withStyles(autocompleteStyles)(
    class Autocomplete<Value, Suggestion> extends React.Component<AutocompleteProps<Value, Suggestion> & WithStyles<AutocompleteClasses>, AutocompleteState> {

    state = {
        suggestionsOpen: false,
        selectedTab: 0,
        selectedSuggestionIndex: 0,
        tabbedListContents: {},
    };

    componentDidUpdate(prevProps: AutocompleteProps<Value, Suggestion>, prevState: AutocompleteState) {
        const { suggestions = [], category } = this.props;
            if( prevProps.suggestions?.length === 0 && suggestions.length > 0) {
                this.setState({ selectedSuggestionIndex: 0, selectedTab: 0 });
            }
            if (category === AutocompleteCat.SHARING) {
                if( prevProps.items.length !== this.props.items.length) {
                    this.setState({ selectedTab: 0, selectedSuggestionIndex: 0 });
                }
                if (Object.keys(this.state.tabbedListContents).length === 0) {
                    this.setState({ tabbedListContents: { groups: [], users: [] } });
                }
                if (prevProps.suggestions !== suggestions) {
                    const users = sortByKey<Suggestion>(suggestions.filter(item => !isUserGroup(item)), 'fullName');
                    const groups = sortByKey<Suggestion>(suggestions.filter(item => isUserGroup(item)), 'name');
                    this.setState({ tabbedListContents: { groups: groups, users: users } });
                }
                if (prevState.selectedTab !== this.state.selectedTab) {
                    this.setState({ selectedSuggestionIndex: 0 });
                }
            }
    }

    containerRef = React.createRef<HTMLDivElement>();
    inputRef = React.createRef<HTMLInputElement>();

    render() {
        return <div ref={this.containerRef}>
                    <FormControl variant="standard" fullWidth error={this.props.error}>
                        {this.renderLabel()}
                        {this.renderInput()}
                        {this.renderHelperText()}
                        {this.props.category === AutocompleteCat.SHARING ? this.renderTabbedSuggestions() : this.renderSuggestions()}
                    </FormControl>
               </div>
        }

    renderLabel() {
        const { label } = this.props;
        return label && <InputLabel>{label}</InputLabel>;
    }

    renderInput() {
        return <Input
            disabled={this.props.disabled}
            autoFocus={this.props.autofocus}
            inputRef={this.inputRef}
            value={this.props.value}
            startAdornment={this.renderChips()}
            onFocus={this.handleFocus}
            onBlur={this.handleBlur}
            onChange={this.props.onChange}
            onKeyPress={this.handleKeyPress}
            onKeyDown={this.handleNavigationKeyPress}
        />;
    }

    renderHelperText() {
        return <FormHelperText>{this.props.helperText}</FormHelperText>;
    }

    renderSuggestions() {
        const { suggestions = [] } = this.props;
        return (
            <Popper
                disablePortal={false}
                modifiers={[
                    maxSize,
                    applyMaxSize
                ]}
                open={this.isSuggestionBoxOpen()}
                anchorEl={this.inputRef.current}
                key={suggestions.length}>
                <Paper onMouseDown={this.preventBlur}>
                    <List dense style={{ width: this.getSuggestionsWidth() }}>
                        {suggestions.map(
                            (suggestion, index) =>
                                <ListItem
                                    button
                                    key={index}
                                    onClick={this.handleSelect(suggestion)}
                                    selected={index === this.state.selectedSuggestionIndex}>
                                    {this.renderSuggestion(suggestion)}
                                </ListItem>
                        )}
                    </List>
                </Paper>
            </Popper>
        );
    }

    renderTabbedSuggestions() {
        const { suggestions = [] } = this.props;

        return (
            <Popper
                disablePortal={false}
                modifiers={[
                    maxSize,
                    applyMaxSize
                ]}
                open={this.state.suggestionsOpen}
                anchorEl={this.containerRef.current || this.inputRef.current}
                key={suggestions.length}
                style={{ width: this.getSuggestionsWidth()}}
            >
                <Paper onMouseDown={this.preventBlur}>
                    <TabbedList
                        tabbedListContents={this.state.tabbedListContents}
                        renderListItem={this.renderSharingSuggestion}
                        selectedIndex={this.state.selectedSuggestionIndex}
                        selectedTab={this.state.selectedTab}
                        handleTabChange={this.handleTabChange}
                        handleSelect={this.handleSelect}
                        includeContentsLength={true}
                        isWorking={this.props.isWorking}
                        maxLength={this.props.maxLength}
                        />
                </Paper>
            </Popper>
        );
    }

    isSuggestionBoxOpen() {
        const { suggestions = [] } = this.props;
        return this.state.suggestionsOpen && suggestions.length > 0;
    }

    handleFocus = (event: React.FocusEvent<HTMLInputElement>) => {
        const { onFocus = noop } = this.props;
        this.setState({ suggestionsOpen: true, selectedTab: 0 });
        onFocus(event);
    }

    handleBlur = (event: React.FocusEvent<HTMLInputElement>) => {
        setTimeout(() => {
            const { onBlur = noop } = this.props;
            this.setState({ suggestionsOpen: false });
            onBlur(event);
        });
    }

    handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
        event.preventDefault();
        this.setState({ selectedTab: newValue });
    };

    handleKeyPress = (event: React.KeyboardEvent<HTMLInputElement>) => {
        const { onCreate = noop, onSelect = noop, suggestions = [] } = this.props;
        const { selectedSuggestionIndex, selectedTab } = this.state;
        if (event.key === 'Enter') {
            if (this.isSuggestionBoxOpen() && selectedSuggestionIndex < suggestions.length) {
                // prevent form submissions when selecting a suggestion
                event.preventDefault();
                if(this.props.category === AutocompleteCat.SHARING) {
                    onSelect(this.state.tabbedListContents[Object.keys(this.state.tabbedListContents)[selectedTab]][selectedSuggestionIndex]);
                } else {
                    onSelect(suggestions[selectedSuggestionIndex]);
                }
            } else if (this.props.value.length > 0) {
                onCreate();
            }
        }
    }

    handleNavigationKeyPress = (ev: React.KeyboardEvent<HTMLInputElement>) => {
        if (ev.key === 'Tab' && this.isSuggestionBoxOpen() && this.props.category === AutocompleteCat.SHARING) {
            ev.preventDefault();
            // Cycle through tabs, or loop back to the first tab
            this.setState({ selectedTab: ((this.state.selectedTab + 1) % Object.keys(this.state.tabbedListContents).length)});
        }
        if (ev.key === 'ArrowUp') {
            ev.preventDefault();
            this.updateSelectedSuggestionIndex(-1);
        } else if (ev.key === 'ArrowDown') {
            ev.preventDefault();
            this.updateSelectedSuggestionIndex(1);
        }
    }

    updateSelectedSuggestionIndex(value: -1 | 1) {
        const { suggestions = [], category } = this.props;
        const { tabbedListContents, selectedTab, selectedSuggestionIndex } = this.state;
        const tabLabels = Object.keys(tabbedListContents);
        const currentList = category === AutocompleteCat.SHARING ? tabbedListContents[tabLabels[selectedTab]] : suggestions;
        if(selectedSuggestionIndex <= 0 && value === -1) {
            this.setState({selectedSuggestionIndex: currentList.length - 1});
        } else {
            this.setState(({ selectedSuggestionIndex }) => ({
                selectedSuggestionIndex: (selectedSuggestionIndex + value) % currentList.length,
                }));
            }
    }

    renderChips() {
        const { items, onDelete } = this.props;

        /**
         * If input startAdornment prop is not undefined, input's label will stay above the input.
         * If there is not items, we want the label to go back to placeholder position.
         * That why we return without a value instead of returning a result of a _map_ which is an empty array.
         */
        if (items.length === 0) {
            return;
        }

        return items.map(
            (item, index) => {
                const tooltip = this.props.renderChipTooltip ? this.props.renderChipTooltip(item) : '';
                if (tooltip && tooltip.length) {
                    return <span key={index}>
                        <Tooltip title={tooltip}>
                        <Chip
                            label={this.renderChipValue(item)}
                            key={index}
                            onDelete={onDelete && !this.props.disabled ? (() =>  onDelete(item, index)) : undefined} />
                    </Tooltip></span>
                } else {
                    return <span key={index}><Chip
                        label={this.renderChipValue(item)}
                        onDelete={onDelete && !this.props.disabled ? (() =>  onDelete(item, index)) : undefined} /></span>
                }
            }
        );
    }

    renderChipValue(value: Value) {
        const { renderChipValue } = this.props;
        return renderChipValue ? renderChipValue(value) : JSON.stringify(value);
    }

    preventBlur = (event: React.MouseEvent<HTMLElement>) => {
        event.preventDefault();
    }

    handleClickAway = (event: React.MouseEvent<HTMLElement>) => {
        if (event.target !== this.inputRef.current) {
            this.setState({ suggestionsOpen: false });
        }
    }

    handleSelect = (suggestion: Suggestion) => {
        return () => {
            const { onSelect = noop } = this.props;
            const { current } = this.inputRef;
            if (current) {
                current.focus();
            }
            onSelect(suggestion);
        };
    }

    renderSuggestion(suggestion: Suggestion) {
        const { renderSuggestion } = this.props;
        return renderSuggestion
            ? renderSuggestion(suggestion)
            : <ListItemText>{JSON.stringify(suggestion)}</ListItemText>;
    }

    renderSharingSuggestion = (suggestion: Suggestion) => {
        return <ListItemText>
                    <Typography className={this.props.classes.listItemStyle} data-cy="sharing-suggestion">
                        { isUserGroup(suggestion) ? `${(suggestion as any).name}` : `${(suggestion as any).fullName} (${(suggestion as any).email})` }
                    </Typography>
                </ListItemText>;
    }

    getSuggestionsWidth() {
        return this.containerRef.current ? this.containerRef.current.offsetWidth : 'auto';
    }
});

const popperMargin = 16; // Space to keep between autocomplete and window edge
const popperPreferredMinHeight = 75; // Roughly 2 autocomplete suggestions

const applyMaxSize = {
    name: "applyMaxSize",
    enabled: true,
    phase: beforeWrite,
    requires: ["maxSize"],
    fn({ state }) {
        // Set maximum usable space with margin
        const { height } = state.modifiersData.maxSize;
        const maxHeight = height - popperMargin;
        state.styles.popper.maxHeight = `${maxHeight}px`;
        state.elements.popper.firstChild.style.maxHeight = `${maxHeight}px`;

        // Get input field bounds and window height
        const referenceElementBounds = state.elements.reference.getBoundingClientRect();
        const windowHeight = window.innerHeight

        if (referenceElementBounds && windowHeight) {
            // Get available space above / below input field
            const spaceAbove = referenceElementBounds.y;
            const spaceBelow = windowHeight - referenceElementBounds.bottom;

            // Swap popper direction if smaller than ~2 suggestions and the alternate direction has more space
            // Space check prevents infinite direction swapping as this gets run again when direction is swapped
            if (maxHeight < popperPreferredMinHeight) {
                if (state.options.placement === 'bottom' && spaceAbove > spaceBelow) {
                    state.options.placement = 'top';
                } else if (state.options.placement === 'top' && spaceBelow > spaceAbove) {
                    state.options.placement = 'bottom';
                }
            }
        }
    }
};

type ChipClasses = 'root';

const chipStyles: CustomStyleRulesCallback<ChipClasses> = theme => ({
    root: {
        marginRight: theme.spacing(0.25),
        height: theme.spacing(3),
    }
});

const Chip = withStyles(chipStyles)(MuiChip);

type PopperClasses = 'root';

const popperStyles: CustomStyleRulesCallback<ChipClasses> = theme => ({
    root: {
        zIndex: theme.zIndex.modal,
    }
});

const Popper = withStyles(popperStyles)(
    ({ classes, ...props }: PopperProps & WithStyles<PopperClasses>) =>
        <MuiPopper {...props} className={classes.root} />
);

type InputClasses = 'root';

const inputStyles: CustomStyleRulesCallback<InputClasses> = () => ({
    root: {
        display: 'flex',
        flexWrap: 'wrap',
    },
    input: {
        minWidth: '20%',
        flex: 1,
    },
});

const Input = withStyles(inputStyles)(MuiInput);

const Paper = withStyles({
    root: {
        maxHeight: '80vh',
        overflowY: 'auto',
    }
})(MuiPaper);
