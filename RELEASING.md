# Releasing

## Why there is a checklist

The first two releases were tagged while every POM still said `0.1.0-SNAPSHOT`. That does not
matter for a jar someone downloads from the releases page, and it matters a great deal for a
published artifact: a version on Maven Central is permanent and cannot be replaced. So the
version bump is now step one, and it is written down rather than remembered.

## What a maintainer needs

Two credentials, neither of which can live in this repository:

- **A Sonatype Central account** with the `io.github.jason-te-sde` namespace verified. GitHub
  namespaces are verified by publishing a repository that Sonatype names, which is why the group
  id is `io.github.jason-te-sde` and not `io.keel`: nobody here controls `keel.io`.
- **A GPG key** whose public half is on a keyserver. Central rejects unsigned artifacts.

Put both in `~/.m2/settings.xml`:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>TOKEN_USERNAME</username>
      <password>TOKEN_PASSWORD</password>
    </server>
  </servers>
  <profiles>
    <profile>
      <id>signing</id>
      <activation><activeByDefault>true</activeByDefault></activation>
      <properties>
        <gpg.keyname>YOUR_KEY_ID</gpg.keyname>
      </properties>
    </profile>
  </profiles>
</settings>
```

## Steps

```bash
# 1. Confirm the tree is releasable. This is the same thing CI runs.
mvn -B verify -Dcoverage

# 2. Run the wide simulation sweep. The per-PR suite runs a dozen seeds; a release should have
#    seen a few thousand.
mvn -B install -DskipTests
mvn -B test -pl keel-testkit -Dkeel.sim.seeds=2000 -Dtest=SoakTest \
    -Dsurefire.failIfNoSpecifiedTests=false -Denforcer.skip=true

# 3. Set the version. Every module, no snapshots.
mvn -B versions:set -DnewVersion=0.3.0 -DgenerateBackupPoms=false

# 4. Update CHANGELOG.md, then commit through a pull request like anything else.
#    main is protected; a release commit is not an exception to that.

# 5. Build the artifacts a consumer needs, without signing, and look at them.
mvn -B -Prelease package -DskipTests
ls keel-raft/target/*.jar     # jar, sources, javadoc

# 6. Sign and upload to the Central staging portal.
mvn -B -Prelease,publish deploy -DskipTests

# 7. Check the deployment in https://central.sonatype.com and publish it there by hand.
#    autoPublish is deliberately off: a published version is permanent.

# 8. Tag. Pushing the tag builds and attaches the runnable jar to the GitHub release.
git tag -a v0.3.0 -m "keel v0.3.0"
git push origin v0.3.0
gh release create v0.3.0 --title "..." --notes-file notes.md --latest

# 9. Open the next development version.
mvn -B versions:set -DnewVersion=0.4.0-SNAPSHOT -DgenerateBackupPoms=false
```

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
