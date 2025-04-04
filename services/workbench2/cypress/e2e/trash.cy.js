// Copyright (C) The Arvados Authors. All rights reserved.
//
// SPDX-License-Identifier: AGPL-3.0

describe('trash tests', function () {
    let adminUser;

    before(function () {
        cy.getUser('admin', 'Admin', 'User', true, true)
            .as('adminUser').then(function () {
                adminUser = this.adminUser;
            });
    });

    it('trashes and untrashes trashable resources (project / collection)', function() {
        // Create test resources
        cy.createProject({
            owningUser: adminUser,
            projectName: `trashTestProject`,
        }).as('testProject');
        cy.createCollection(adminUser.token, {
            owner_uuid: adminUser.user.uuid,
            name: `trashTestCollection ${Math.floor(Math.random() * 999999)}`,
        }).as('testCollection');

        cy.getAll('@testProject', '@testCollection')
            .then(function ([testProject, testCollection]) {
                cy.loginAs(adminUser);

                // Project Trash Tests

                // Trash with context menu
                cy.doDataExplorerContextAction(testProject.name, 'Move to trash');

                // Verify trashed and breadcrumbs correct
                cy.assertDataExplorerContains(testProject.name, false);
                cy.assertBreadcrumbs(["Home Projects"]);

                // Restore with context menu
                cy.get('[data-cy=side-panel-tree]').contains('Trash').click();
                cy.assertBreadcrumbs(["Trash"]);
                cy.doDataExplorerContextAction(testProject.name, 'Restore');

                // Verify navigated to project
                cy.assertBreadcrumbs(["Home Projects", testProject.name]);
                cy.assertUrlPathname(`/projects/${testProject.uuid}`);
                // Verify present in home project
                cy.get('[data-cy=side-panel-tree]').contains('Home Projects').click();
                cy.assertBreadcrumbs(["Home Projects"]);
                cy.assertDataExplorerContains(testProject.name, true);

                // Test delete from toolbar
                cy.doDataExplorerSelect(testProject.name);
                cy.doToolbarAction("Move to trash");

                // Verify trashed and breadcrumbs correct
                cy.get('[data-cy=data-table]').contains(testProject.name).should('not.exist');
                cy.assertBreadcrumbs(["Home Projects"]);

                // Restore with toolbar
                cy.get('[data-cy=side-panel-tree]').contains('Trash').click();
                cy.assertBreadcrumbs(["Trash"]);
                cy.doDataExplorerSelect(testProject.name);
                cy.doToolbarAction("Restore");

                // Verify navigated to project
                cy.assertBreadcrumbs(["Home Projects", testProject.name]);
                cy.assertUrlPathname(`/projects/${testProject.uuid}`);
                // Verify present in home project
                cy.get('[data-cy=side-panel-tree]').contains('Home Projects').click();
                cy.assertBreadcrumbs(["Home Projects"]);
                cy.get('[data-cy=data-table]').contains(testProject.name).should('exist');

                // Collection Trash Tests

                // Trash with context menu
                cy.doDataExplorerContextAction(testCollection.name, 'Move to trash');

                // Verify trashed and breadcrumbs correct
                cy.get('[data-cy=data-table]').contains(testCollection.name).should('not.exist');
                cy.assertBreadcrumbs(["Home Projects"]);

                // Restore with context menu
                cy.get('[data-cy=side-panel-tree]').contains('Trash').click();
                cy.assertBreadcrumbs(["Trash"]);
                cy.doDataExplorerContextAction(testCollection.name, 'Restore');

                // Verify not in trash and in home project
                cy.get('[data-cy=data-table]').contains(testCollection.name).should('not.exist');
                cy.assertBreadcrumbs(["Trash"]);
                cy.get('[data-cy=side-panel-tree]').contains('Home Projects').click();
                cy.assertBreadcrumbs(["Home Projects"]);
                cy.get('[data-cy=data-table]').contains(testCollection.name).should('exist');

                // Test delete from toolbar
                cy.doDataExplorerSelect(testCollection.name);
                cy.doToolbarAction("Move to trash");

                // Verify trashed and breadcrumbs correct
                cy.get('[data-cy=data-table]').contains(testCollection.name).should('not.exist');
                cy.assertBreadcrumbs(["Home Projects"]);

                // Restore with toolbar
                cy.get('[data-cy=side-panel-tree]').contains('Trash').click();
                cy.assertBreadcrumbs(["Trash"]);
                cy.doDataExplorerSelect(testCollection.name);
                cy.doToolbarAction("Restore");

                // Verify not in trash and in home project
                cy.get('[data-cy=data-table]').contains(testCollection.name).should('not.exist');
                cy.assertBreadcrumbs(["Trash"]);
                cy.get('[data-cy=side-panel-tree]').contains('Home Projects').click();
                cy.assertBreadcrumbs(["Home Projects"]);
                cy.get('[data-cy=data-table]').contains(testCollection.name).should('exist');
            });
    });
});
