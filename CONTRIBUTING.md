# Contributing

## Prerequisites

- JDK 21 or newer, Maven 3.9 or newer. Nothing else: `protoc` and the gRPC
  code generator are downloaded by the build.
- `mvn verify` must pass before a pull request is opened. It compiles with
  `-Werror`, so a warning is a build failure.

## Workflow

1. Open an issue first. It states the problem, the proposed approach, and
   acceptance criteria. If the change touches replication or durability, name the
   invariant it is meant to preserve.
2. Branch from `main`. Branch names are `<type>/<issue-number>-<short-slug>`, for
   example `feat/5-readindex` or `fix/31-snapshot-truncation`.
3. Commit in small steps that each build and pass tests.
4. Open a pull request that closes the issue (`Closes #5`).
5. Squash merge, so `main` keeps one commit per pull request and its history
   reads as a list of changes rather than a list of keystrokes.

`main` is protected: CI must be green and the branch must be up to date before
merging.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/), with the module as
the scope:

```
<type>(<scope>): <imperative summary, no trailing period>

<body: why this change, what was considered and rejected, what it does not do>

Refs #<issue>
```

Types in use: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `ci`, `chore`.
Scopes are module names without the `keel-` prefix (`raft`, `storage`, `kv`,
`node`, `proto`, `testkit`) or `build`.

The summary says what the change does, not what you did:

```
good: feat(raft): confirm leadership with a heartbeat round before serving reads
bad:  fixed some bugs in raft and added tests
```

A one-line commit is fine for a rename. Write a body when the reasoning is not
visible in the diff.

## Tests

Every behavioural change needs a test that fails without it. The suite has four
layers, and new code should land in the cheapest layer that can catch its bugs:

| Layer | Location | What it covers |
| --- | --- | --- |
| Unit | `<module>/src/test/java` | One method or one state transition |
| Deterministic network | `keel-raft` tests | Multi-node message exchange with no clocks or threads |
| Simulation | `keel-testkit` | Seeded fault injection with invariant checks after every step |
| Integration | `keel-node` tests | Real sockets, real files, real threads |

Rules that matter in practice:

- Tests must not sleep to wait for progress. Use the deterministic tick API, or
  poll a condition with a timeout, so the suite behaves the same on a loaded CI
  runner as it does on a laptop.
- Anything randomized takes a seed and prints it on failure. A red test that
  cannot be replayed is nearly worthless.
- Assertions carry a message that identifies the state, not just the expected
  value. `assertEquals(3, term)` tells a reader nothing at 2am.

When a simulation failure is fixed, add its seed to the regression list so it is
replayed on every run.

## Code style

- Four spaces, 100 column limit, one top-level type per file. The existing code
  is the reference; there is no formatter plugin to argue with.
- `final` on fields that never change, and immutable value types (records) for
  anything crossing a module boundary.
- Every public type and method has Javadoc. Private methods get a comment only
  when the reason for them is not obvious.
- Comments explain why, not what. If a line needs a comment to say what it does,
  the line is usually the problem.
- Where the code follows the Raft paper, cite the section (for example
  "5.4.1 Election Restriction") instead of paraphrasing the argument.
- Exceptions carry enough context to locate the caller. `IllegalStateException`
  is for violated internal invariants, `IllegalArgumentException` for bad input
  from a caller, and neither is ever used for control flow.
- No wildcard imports, no static state, no `System.out` outside the CLI.
