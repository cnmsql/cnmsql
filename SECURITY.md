# Security Policy

We take security issues in CNMSQL seriously and appreciate the work of those who report them responsibly.

## Reporting a vulnerability

Do not open a public issue for a security problem. Public reports expose users before a fix exists.

Report privately through one of these channels:

- **GitHub private vulnerability reporting.** Go to the [Security tab](https://github.com/cnmsql/cnmsql/security/advisories/new) and open a draft advisory. This is the preferred route.
- **Email.** Write to the maintainers at security@cnmsql.co.

Include as much as you can:

- The affected version or commit.
- A description of the issue and its impact.
- Steps or a proof of concept that reproduces it.
- Any configuration needed to trigger it.

## What to expect

- We acknowledge your report within three business days.
- We confirm the issue and tell you our assessment of its severity.
- We keep you updated as we work on a fix and credit you in the advisory unless you ask us not to.
- We coordinate a disclosure date with you once a fix is ready.

Please give us a reasonable window to release a fix before any public disclosure.

## Supported versions

Security fixes land on the latest released minor version. Older versions may not receive backports, so we recommend running a current release.

## Scope

This policy covers the operator, the instance images, and the `kubectl cnmsql` plugin in this repository. Issues in Percona Server for MySQL, Kubernetes, or other upstream dependencies should go to their respective projects, though we are glad to help route a report if you are unsure.
