# Releasing

## Why there is a checklist

The first two releases were tagged while every POM still said `0.1.0-SNAPSHOT`. That does not
matter for a jar someone downloads from the releases page, and it matters a great deal for a
published artifact: a version on Maven Central is permanent and cannot be replaced. So the
version bump is now step one, and it is written down rather than remembered.

## What a maintainer needs

Two credentials, neither of which can live in this repository, both set up once and then left
alone.

- **A Sonatype Central account** with the `io.github.jason-te-sde` namespace verified. GitHub
  namespaces are verified by publishing a repository that Sonatype names, which is why the group
  id is `io.github.jason-te-sde` and not `io.keel`: nobody here controls `keel.io`.
- **A GPG key** whose public half is on a keyserver. Central rejects unsigned artifacts, and it
  fetches the public key to check them, so a key that only exists on one laptop is not enough.

They live in the `maven-central` GitHub environment as four secrets:

| Secret | What it is |
| --- | --- |
| `CENTRAL_TOKEN_USERNAME` | user token from the Central portal, not the account password |
| `CENTRAL_TOKEN_PASSWORD` | the other half of that token |
| `GPG_PRIVATE_KEY` | the ASCII-armored private key, `-----BEGIN` line included |
| `GPG_PASSPHRASE` | the passphrase protecting it |

`SETUP-PUBLISHING.md` has the exact commands for producing all four.

Publishing runs in CI rather than off a laptop. Not for convenience: a release built on a
developer machine is built against whatever happens to be installed there, and the one artifact
that can never be corrected afterwards is the wrong place to find that out.

## Steps

Steps 1 to 4 are a normal change and go through a pull request like anything else. `main` is
protected and a release commit is not an exception.

```bash
# 1. Confirm the tree is releasable, on every JDK in the CI matrix.
scripts/preflight.sh

# 2. Run the wide simulation sweep. The per-pull-request suite runs a dozen seeds; a release
#    should have seen several thousand. The four safety bugs fixed in v0.3.1 were all found
#    between 500 and 10,000 seeds, so this number is not decoration.
mvn -B install -DskipTests
mvn -B test -pl keel-testkit -Dkeel.sim.seeds=10000 -Dtest=SoakTest \
    -Dsurefire.failIfNoSpecifiedTests=false -Denforcer.skip=true

# 3. Set the version. Every module, no snapshots.
mvn -B versions:set -DnewVersion=0.4.0 -DgenerateBackupPoms=false

# 4. Update CHANGELOG.md and the version in README.md's dependency snippet, then open the
#    pull request and merge it.
```

Then, once that is on `main`:

```bash
# 5. Publish. This stages the deployment; it does not make it permanent.
gh workflow run publish.yml -f version=0.4.0

# 6. Open https://central.sonatype.com/publishing/deployments, check that all six modules are
#    there with a jar, sources, javadoc and signature each, and press Publish.
#    autoPublish is deliberately off. A published version can never be replaced.

# 7. Tag. Pushing the tag builds the runnable jar and attaches it to the GitHub release.
git tag -a v0.4.0 -m "keel v0.4.0"
git push origin v0.4.0
gh release create v0.4.0 --title "..." --notes-file notes.md --latest

# 8. Open the next development version.
mvn -B versions:set -DnewVersion=0.5.0-SNAPSHOT -DgenerateBackupPoms=false
```

### Publishing from a laptop instead

Possible, and not the recommended path. Put the token in `~/.m2/settings.xml` under the server id
`central`, have the signing key in the local keyring, and run:

```bash
MAVEN_GPG_PASSPHRASE=... mvn -B -Prelease,publish deploy -DskipTests
```

The gpg plugin is configured with `--pinentry-mode loopback`, so it reads the passphrase from that
variable rather than opening a dialog. Without it, signing on a machine with no terminal does not
fail; it hangs.

## What gets published

| Artifact | Contents |
| --- | --- |
| `keel-raft` | the consensus core, and the only module a library consumer usually wants |
| `keel-storage` | the write-ahead log |
| `keel-kv` | the key-value state machine |
| `keel-testkit` | the simulator, invariant checks, and linearizability checker |
| `keel-node` | the server, client, and CLI |
| `keel-proto` | generated schema classes; a transitive dependency of the rest |

`keel-node/target/keel.jar` is the runnable jar and is attached to the GitHub release rather than
published to Central. It is a shaded uber-jar, which is the right shape for running and the wrong
shape for depending on.

## Versioning

Semantic versioning, with one caveat stated plainly: **before 1.0 both the wire format and the
on-disk format may change between minor versions.** There is no migration tooling. An upgrade
across a minor version means a fresh data directory, and `docs/operations.md` says the same thing
where an operator will actually read it.
