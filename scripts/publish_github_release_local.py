"""Build, stage, and publish a verified GitHub release without Actions."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from verify_release_artifacts import (
    MANIFEST_NAME,
    create_manifest,
    digest,
    distribution_files,
    release_version,
    verify_release,
)


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "umf-ashlar"
REPOSITORY_PATTERN = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")


def run(command: list[str], *, cwd: Path = ROOT, capture: bool = True) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    return result.stdout.strip() if capture else ""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def check_source(tag: str, expected_commit: str, repository: str) -> str:
    version = release_version(tag)
    require(bool(REPOSITORY_PATTERN.fullmatch(repository)), "owner/repository required")
    head = run(["git", "rev-parse", "HEAD"])
    require(head == expected_commit, "HEAD does not match --expected-commit")
    tagged = run(["git", "rev-parse", f"refs/tags/{tag}^{{commit}}"])
    require(tagged == expected_commit, "tag does not resolve to --expected-commit")
    remote_tags = run(
        ["git", "ls-remote", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}"]
    )
    remote_commits = {line.split()[0] for line in remote_tags.splitlines() if line.split()}
    require(expected_commit in remote_commits, "origin tag does not resolve to --expected-commit")
    local_main = run(["git", "rev-parse", "refs/remotes/origin/main"])
    remote_main_lines = run(["git", "ls-remote", "origin", "refs/heads/main"])
    remote_main = remote_main_lines.split()[0] if remote_main_lines.split() else ""
    require(local_main == remote_main, "fetch origin/main before release preflight")
    require(expected_commit == remote_main, "release source must equal current origin/main")
    status = run(["git", "status", "--porcelain", "--untracked-files=all"])
    require(not status, "source checkout must be clean before release build")
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    package_version = re.search(r'^version\s*=\s*"([^"]+)"\s*$', project, re.M)
    require(
        package_version is not None and package_version.group(1) == version,
        f"pyproject version must match {tag}",
    )
    return expected_commit


def build_and_manifest(
    directory: Path, *, tag: str, repository: str, source_commit: str
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    run(["uv", "build", "--out-dir", str(directory)])
    return create_manifest(
        directory,
        directory / MANIFEST_NAME,
        tag=tag,
        repository=repository,
        source_commit=source_commit,
        package=PACKAGE,
    )


def release_view(tag: str, repository: str) -> dict[str, Any]:
    raw = run(
        [
            "gh",
            "release",
            "view",
            tag,
            "--repo",
            repository,
            "--json",
            "tagName,isDraft,isPrerelease,isImmutable,publishedAt,assets",
        ]
    )
    value = json.loads(raw)
    require(value.get("tagName") == tag, "GitHub release tag mismatch")
    return value


def verify_remote_assets(release: dict[str, Any], directory: Path) -> None:
    local = {
        path.name: f"sha256:{digest(path)}"
        for path in [*distribution_files(directory, PACKAGE, release_version(release["tagName"])), directory / MANIFEST_NAME]
    }
    remote_assets = release.get("assets")
    require(isinstance(remote_assets, list), "GitHub asset inventory unavailable")
    remote: dict[str, str] = {}
    for asset in remote_assets:
        name, checksum = asset.get("name"), asset.get("digest")
        require(isinstance(name, str) and isinstance(checksum, str), "GitHub asset digest unavailable")
        require(name not in remote, "duplicate GitHub release asset")
        remote[name] = checksum
    require(remote == local, "GitHub release assets differ from locally verified build")


def download_and_verify_draft(
    tag: str, repository: str, source_commit: str, release: dict[str, Any], directory: Path
) -> dict[str, Any]:
    require(release.get("isDraft") is True, "only a draft release can be published")
    run(["gh", "release", "download", tag, "--repo", repository, "--dir", str(directory)])
    files = distribution_files(directory, PACKAGE, release_version(tag))
    manifest_path = directory / MANIFEST_NAME
    require(manifest_path.is_file(), "release manifest asset required")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    require(
        manifest.get("schema") == "github-pypi-release/1"
        and manifest.get("repository") == repository
        and manifest.get("tag") == tag
        and manifest.get("source_commit") == source_commit
        and manifest.get("package") == PACKAGE
        and manifest.get("version") == release_version(tag),
        "draft manifest source or package identity mismatch",
    )
    expected = {entry.get("name"): entry.get("sha256") for entry in manifest.get("artifacts", [])}
    actual = {path.name: digest(path) for path in files}
    require(expected == actual, "draft distributions do not match the source manifest")
    verify_remote_assets(release, directory)
    return manifest


def check_repository_identity(repository: str) -> None:
    actual = run(["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"])
    require(actual.lower() == repository.lower(), "gh active repository does not match --repository")


def check_immutable_releases(repository: str) -> dict[str, Any]:
    raw = run(["gh", "api", f"repos/{repository}/immutable-releases"])
    policy = json.loads(raw)
    require(policy.get("enabled") is True, "GitHub immutable releases must be enabled")
    return policy


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build and publish a stable GitHub release from a reviewed local tag."
    )
    parser.add_argument("action", choices=("preflight", "stage", "publish"))
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--repository", required=True, help="GitHub owner/repository")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="perform the requested GitHub stage/publish action; default is a dry run",
    )
    parser.add_argument(
        "--confirm-publish",
        help="must exactly match --tag when publishing a draft GitHub release",
    )
    args = parser.parse_args()
    try:
        if args.action == "publish":
            require(args.confirm_publish == args.tag, "publish requires --confirm-publish matching --tag")
            require(args.execute, "publish requires --execute")
        check_source(args.tag, args.expected_commit, args.repository)
        check_repository_identity(args.repository)
        check_immutable_releases(args.repository)
        with tempfile.TemporaryDirectory(prefix="ashlar-release-") as temporary:
            directory = Path(temporary)
            if args.action in ("preflight", "stage"):
                result = build_and_manifest(
                    directory,
                    tag=args.tag,
                    repository=args.repository,
                    source_commit=args.expected_commit,
                )
                summary = {"action": args.action, **result}
            else:
                release = release_view(args.tag, args.repository)
                result = download_and_verify_draft(
                    args.tag, args.repository, args.expected_commit, release, directory
                )
                summary = {"action": args.action, "manifest": result}

            if args.action == "stage":
                if args.execute:
                    run(
                        [
                            "gh",
                            "release",
                            "create",
                            args.tag,
                            "--repo",
                            args.repository,
                            "--verify-tag",
                            "--draft",
                            "--title",
                            f"Ashlar {args.tag}",
                            "--generate-notes",
                            *[str(path) for path in sorted(directory.iterdir())],
                        ],
                        capture=False,
                    )
                    release = release_view(args.tag, args.repository)
                    require(release.get("isDraft") is True, "new release is not a draft")
                    verify_remote_assets(release, directory)
                    summary["staged"] = True
                else:
                    summary["dry_run"] = True
                    summary["would_run"] = "gh release create --verify-tag --draft"

            elif args.action == "publish":
                run(
                    ["gh", "release", "edit", args.tag, "--repo", args.repository, "--draft=false"],
                    capture=False,
                )
                published = release_view(args.tag, args.repository)
                require(
                    published.get("isDraft") is False
                    and published.get("isPrerelease") is False
                    and published.get("isImmutable") is True
                    and published.get("publishedAt"),
                    "GitHub did not return a published immutable stable release",
                )
                receipt_directory = Path(temporary) / "receipt"
                receipt_directory.mkdir()
                release_path = receipt_directory / "published-release.json"
                release_payload = {
                    "tag_name": published["tagName"],
                    "draft": published["isDraft"],
                    "prerelease": published["isPrerelease"],
                    "immutable": published["isImmutable"],
                    "published_at": published["publishedAt"],
                    "assets": published["assets"],
                }
                release_path.write_text(json.dumps(release_payload), encoding="utf-8")
                verify_release(
                    release_path,
                    directory,
                    tag=args.tag,
                    repository=args.repository,
                    source_commit=args.expected_commit,
                    package=PACKAGE,
                )
                summary["published"] = True

            print(json.dumps(summary, sort_keys=True))
    except (ValueError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"release {args.action} refused: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
