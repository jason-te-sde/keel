# Setting up publishing

One-time setup, done by a maintainer. It produces four GitHub secrets; after that, publishing a
release is `gh workflow run publish.yml -f version=X.Y.Z` followed by one button in the Central
portal.

Everything here happens outside this repository. Nothing produced by these steps should ever be
committed.

---

## 1. Claim the namespace on Sonatype Central

The group id is `io.github.jason-te-sde`, which Central verifies by checking that you control the
GitHub account of the same name. That is the whole reason for the `io.github.` prefix: verifying
`io.keel` would mean proving ownership of the domain `keel.io`, which nobody here has.

1. Go to <https://central.sonatype.com> and **Sign in with GitHub**.
2. Open **Namespaces**. `io.github.jason-te-sde` should already be listed, unverified.
3. Press **Verify Namespace**. Central shows a repository name made of random characters, something
   like `abc123xyz0`.
4. Create a public repository with exactly that name under your account. It can be empty:

   ```sh
   gh repo create abc123xyz0 --public --description "Sonatype Central namespace verification"
   ```

5. Back in the portal, press the confirm button. Verification is usually immediate.
6. Delete the repository once the namespace shows as verified:

   ```sh
   gh repo delete abc123xyz0 --yes
   ```

**Check before moving on:** the Namespaces page lists `io.github.jason-te-sde` as verified.

---

## 2. Generate a user token

Central does not accept your account password for uploads. It issues a token that looks like a
username and password pair.

1. In the portal, open the account menu, then **View Account**, then **Generate User Token**.
2. It shows a `<username>` and `<password>` block **once**. Copy both somewhere safe now; the
   password is not shown again.

These become `CENTRAL_TOKEN_USERNAME` and `CENTRAL_TOKEN_PASSWORD`.

---

## 3. Create a signing key

Central rejects unsigned artifacts, and it fetches your public key from a keyserver to check the
signatures, so the key has to be published as well as created.

```sh
gpg --full-generate-key
```

Answer:

- kind of key: **(1) RSA and RSA**
- keysize: **4096**
- expiry: **0** (does not expire) — a key that expires makes every past release unverifiable
- real name: your name
- email: the address you want associated with the artifacts
- passphrase: pick one and keep it; you will need it in step 4

Find the key id:

```sh
gpg --list-secret-keys --keyid-format=long
```

```
sec   rsa4096/A1B2C3D4E5F67890 2026-08-05 [SC]
      ^^^^^^^^^^^^^^^^^^^^^^^^ the part after the slash is the key id
```

Publish the public half. Do all three; keyservers do not reliably sync with each other, and
Central only needs to find it on one:

```sh
KEYID=A1B2C3D4E5F67890
gpg --keyserver keyserver.ubuntu.com --send-keys "$KEYID"
gpg --keyserver keys.openpgp.org     --send-keys "$KEYID"
gpg --keyserver pgp.mit.edu          --send-keys "$KEYID"
```

**Check before moving on**, and give it a minute or two to propagate:

```sh
gpg --keyserver keyserver.ubuntu.com --recv-keys "$KEYID"
```

---

## 4. Export the private key

GitHub Actions needs the private half to sign with. This is the one genuinely sensitive value
here.

```sh
gpg --armor --export-secret-keys "$KEYID" | pbcopy
```

That puts it on the clipboard. It starts with `-----BEGIN PGP PRIVATE KEY BLOCK-----` and ends with
`-----END PGP PRIVATE KEY BLOCK-----`, and both of those lines are part of it.

If your GnuPG refuses without a passphrase prompt it can display, add:

```sh
gpg --armor --pinentry-mode loopback --passphrase 'YOUR_PASSPHRASE' \
    --export-secret-keys "$KEYID" | pbcopy
```

Do not write it to a file inside this repository, even briefly. If you need a file, put it in
`/tmp` and delete it afterwards.

---

## 5. Put the four values into GitHub

The publish workflow reads them from an environment named `maven-central`. Using an environment
rather than plain repository secrets means the credentials are not readable by a workflow that
merely runs on a pull request.

1. <https://github.com/jason-te-sde/keel/settings/environments>
2. **New environment**, named exactly `maven-central`.
3. Add four **environment secrets**:

| Name | Value |
| --- | --- |
| `CENTRAL_TOKEN_USERNAME` | the `<username>` from step 2 |
| `CENTRAL_TOKEN_PASSWORD` | the `<password>` from step 2 |
| `GPG_PRIVATE_KEY` | the whole armored block from step 4 |
| `GPG_PASSPHRASE` | the passphrase from step 3 |

The names have to match exactly; the workflow looks them up by name.

Or from the command line:

```sh
gh secret set CENTRAL_TOKEN_USERNAME --env maven-central
gh secret set CENTRAL_TOKEN_PASSWORD --env maven-central
gh secret set GPG_PRIVATE_KEY        --env maven-central < /tmp/private-key.asc
gh secret set GPG_PASSPHRASE         --env maven-central
```

---

## 6. Publish

```sh
gh workflow run publish.yml -f version=0.3.1
gh run watch
```

The workflow refuses to run if the version you passed does not match the POMs, or if it is a
snapshot. It then builds, tests, signs, and uploads.

**It does not make the release permanent.** Open
<https://central.sonatype.com/publishing/deployments>, confirm that all six modules are there and
that each has a jar, a sources jar, a javadoc jar and a `.asc` signature beside it, and press
**Publish**.

That last step is a person's decision on purpose. A version on Central can never be replaced or
removed, so it is worth looking at the file list once.

Artifacts appear in search within minutes and are resolvable by Maven within a few hours.

---

## What was already checked

The signing half of this was tested end to end with a throwaway key, so the parts that can fail
without credentials have been ruled out:

- All six modules produce a jar, sources and javadoc, and the parent produces its POM. Central
  rejects a bundle missing any of them, and `keel-proto` was missing its javadoc until #64.
- Every module's effective POM has `name`, `description`, `url`, `licenses`, `scm` and
  `developers`, all of which Central validates.
- `mvn -Prelease,publish verify` produced 25 signature files, four per module plus the parent POM,
  and `gpg --verify` accepted them.
- The gpg plugin is configured with `--pinentry-mode loopback`, without which signing on a machine
  with no terminal hangs waiting for a dialog instead of failing.

What has **not** been exercised is the upload itself, because that needs the credentials above.
The first run of step 6 is the first time that path executes.
