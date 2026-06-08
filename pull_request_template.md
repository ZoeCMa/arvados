<!-- template begins below, replace the bits between < > -->

`<0000-branch-title>` @ <git hash>

<https://ci.arvados.org/... (link to developer test job on jenkins)>

_Note each item completed with additional detail if necessary.  If an item is irrelevant to a specific branch, briefly explain why._

* All agreed upon points are implemented / addressed.  Describe changes from pre-implementation design.
  * _comments_
* Anything not implemented (discovered or discussed during work) has a follow-up story.
  * _comments_
* Code is tested and passing, both automated and manual, what manual testing was done is described.
  * _comments_
* The tested code incorporates recent main branch changes.
  * _confirm_ <!--"Incorporates" = merged or rebased. "Recent" = 2-3 working days. The more active development on this component is, the more important it is to be based on recent main to avoid surprising test failures post-merge.-->
* New or changed UI/UX has gotten feedback from stakeholders.
  * _comments_
* Documentation has been updated.
  * _comments_
* Behaves appropriately at the intended scale (describe intended scale).
  * _comments_
* Considered backwards and forwards compatibility issues between client and server.
  * _comments_
* Follows our [coding standards](https://github.com/arvados/arvados/blob/main/doc/development/CodingStandards.md) and [GUI style guidelines](https://github.com/arvados/arvados/blob/main/doc/development/CodingStandards.md#workbench-design-guidelines)
  * _comments_

<Additional detail about what, why and how this branch changes the code>

Closes <#0000>.
