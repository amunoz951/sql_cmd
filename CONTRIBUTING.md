# How to contribute

Adding operating systems and other functionality would be very welcome.

## Getting Started

* Create an issue in the [GitHub repo](https://github.com/amunoz951/sql_cmd/issues)
  * Clearly describe the issue including steps to reproduce when it is a bug.
  * Make sure you fill in the earliest version that you know has the issue.
  * A ticket is not necessary for trivial changes

## Making Changes

* Create a new branch and make your changes.
* Make commits of logical and atomic units.
* Run cookstyle and foodcritic and correct any errors before committing.
* Make sure your commit messages are in the proper format. If the commit
  addresses an issue filed in the
  [GitHub repo](https://github.com/amunoz951/sql_cmd/issues), start
  the first line of the commit with the issue number in parentheses.

  ```
      (PUP-1234) Make the example in CONTRIBUTING imperative and concrete

      Without this patch applied the example commit message in the CONTRIBUTING
      document is not a concrete example. This is a problem because the
      contributor is left to imagine what the commit message should look like
      based on a description rather than an example. This patch fixes the
      problem by making the example concrete and imperative.

      The first line is a real-life imperative statement with a ticket number
      from our issue tracker. The body describes the behavior without the patch,
      why this is a problem, and how the patch fixes the problem when applied.
  ```
* Make sure you have added the necessary tests for your changes in the included
  sql_cmd_test cookbook and the inspec profile.

## Making Trivial Changes

For trivial changes, it is not always necessary to create a new
ticket in Jira. In this case, it is appropriate to start the first line of a
commit with one of  `(docs)` or `(maint)` instead of a ticket
number.

If a Jira ticket exists for the documentation commit, you can include it
after the `(docs)` token.

```
    (docs)(DOCUMENT-000) Add docs commit example to CONTRIBUTING

    There is no example for contributing a documentation commit
    to the repository. This is a problem because the contributor
    is left to assume how a commit of this nature may appear.

    The first line is a real-life imperative statement with '(docs)' in
    place of what would have been the ticket number in a non-documentation
    related commit. The body describes the nature of the new documentation
    or comments added.
```

For commits that address trivial repository maintenance tasks, start the
first line of the commit with `(maint)`.

## Submitting Changes

* Push your new branch and changes to origin.
* Submit a pull request to master branch.
* Update your ticket to mark that you have submitted code and are ready
  for it to be reviewed (Status: Ready for Merge).
  * Include a link to the pull request in the ticket.
* The cookbook author will code review and determine if it's ready for
  approval/merge. If it's determined that more work is needed, the cookbook
  author will provide feedback.
* After feedback has been given we expect responses within two weeks. After two
  weeks we may close the pull request if it isn't showing any activity.

## Revert Policy

By running kitchen tests in advance and by engaging with peer review for
prospective changes, your contributions have a high probability of becoming long
lived parts of the the project.

If the code change breaks any existing functionality and a fix cannot be
determined and committed within 24 hours of its discovery, the commit(s)
responsible _may_ be reverted, at the discretion of the committer and cookbook
author.

The original contributor will be notified of the revert in the GitHub ticket
associated with the change. An explanation of what failed as a result of the
code change will also be added to the GitHub ticket.
