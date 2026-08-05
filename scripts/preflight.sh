#!/bin/sh
# Runs what CI runs, on every JDK in the CI matrix, before you push.
#
# Usage: scripts/preflight.sh [extra maven arguments]
#
# The build compiles with -Werror, and javac's lint categories change between releases: a comment or
# a construct that is silent on one JDK is a build failure on the next. Two changes have gone red on
# JDK 25 only, after passing locally on 21. Running `mvn verify` on whichever JDK happens to be first
# on your PATH does not tell you whether CI will be green, and this does.
#
# A JDK it cannot find is treated as a failure rather than a note, because a preflight that quietly
# checks half the matrix is worse than none: it produces the confidence without the coverage. Set
# KEEL_PREFLIGHT_ALLOW_MISSING=1 to downgrade that to a warning.
set -eu

# Keep in step with the matrix in .github/workflows/ci.yml.
VERSIONS='21 25'

# Prints the JAVA_HOME for a major version, or nothing.
#
# Every candidate is confirmed by asking the java binary what version it is, because the directory
# names are a convention rather than a promise and a wrong match here would be reported as a pass.
find_jdk() {
    version=$1

    # An explicit JAVA21_HOME or JAVA25_HOME wins: someone who set it meant it.
    explicit=$(eval "printf '%s' \"\${JAVA${version}_HOME:-}\"")
    candidates=$explicit

    if command -v /usr/libexec/java_home >/dev/null 2>&1; then
        candidates="$candidates $(/usr/libexec/java_home -v "$version" 2>/dev/null || true)"
    fi

    for dir in \
        "$HOME/Library/Java/JavaVirtualMachines"/*"$version"*/Contents/Home \
        /Library/Java/JavaVirtualMachines/*"$version"*/Contents/Home \
        /usr/lib/jvm/*"$version"* \
        /opt/homebrew/opt/openjdk@"$version"/libexec/openjdk.jdk/Contents/Home; do
        [ -d "$dir" ] && candidates="$candidates $dir"
    done

    for candidate in $candidates; do
        [ -x "$candidate/bin/java" ] || continue
        # The first quoted number on the first line, so `java version "21.0.8" 2025-07-15 LTS` and
        # `openjdk version "25.0.4" 2026-07-21 LTS` both give the major version. Anchoring at the
        # start matters: a greedy match runs past the closing quote and yields nothing.
        actual=$("$candidate/bin/java" -version 2>&1 | head -1 | sed 's/^[^"]*"\([0-9]*\).*/\1/')
        if [ "$actual" = "$version" ]; then
            printf '%s' "$candidate"
            return 0
        fi
    done
    return 1
}

missing=''
found=''
for version in $VERSIONS; do
    if home=$(find_jdk "$version"); then
        found="$found $version=$home"
    else
        missing="$missing $version"
    fi
done

if [ -n "$missing" ]; then
    echo "preflight: no JDK found for:$missing" >&2
    echo >&2
    echo "Install one without root, for example JDK 25 on an Apple Silicon Mac:" >&2
    echo >&2
    echo "  mkdir -p ~/Library/Java/JavaVirtualMachines" >&2
    echo "  curl -sSL 'https://api.adoptium.net/v3/binary/latest/25/ga/mac/aarch64/jdk/hotspot/normal/eclipse' \\" >&2
    echo "    | tar xz -C ~/Library/Java/JavaVirtualMachines" >&2
    echo >&2
    echo "Or point JAVA25_HOME at one you already have." >&2
    if [ "${KEEL_PREFLIGHT_ALLOW_MISSING:-0}" != '1' ]; then
        exit 1
    fi
    echo "preflight: continuing anyway, because KEEL_PREFLIGHT_ALLOW_MISSING is set" >&2
fi

failed=''
for entry in $found; do
    version=${entry%%=*}
    home=${entry#*=}

    echo
    echo "════ jdk $version ════ $home"
    if JAVA_HOME="$home" mvn -B -ntp verify "$@"; then
        echo "jdk $version: ok"
    else
        echo "jdk $version: FAILED" >&2
        failed="$failed $version"
    fi
done

echo
if [ -n "$failed" ]; then
    echo "preflight failed on jdk:$failed" >&2
    exit 1
fi
echo "preflight passed on jdk:$(echo "$found" | sed 's/=[^ ]*//g')"
