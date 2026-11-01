"""Build, stage, and publish a verified GitHub release without Actions."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

from verify_release_artifacts import (
    MANIFEST_NAME,
    create_manifest,
    digest,
    distribution_files,
    release_version,
    validate_builder_identity,
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


def _version_tuple(value: str) -> tuple[int, ...]:
    match = re.match(r"^(\d+(?:\.\d+)*)", value) if type(value) is str else None
    require(match is not None, "selected build environment has an invalid version")
    return tuple(int(part) for part in match.group(1).split("."))


def selected_builder_identity(python_path: Path) -> dict[str, Any]:
    """Validate and retain the explicitly selected no-isolation build runtime."""
    require(isinstance(python_path, Path) and python_path.is_absolute()
            and python_path.is_file() and os.access(python_path, os.X_OK),
            "--build-python must name an executable absolute Python path")
    probe = (
        "import importlib.metadata as m,json,platform,sys; "
        "get=lambda n: (m.version(n) if next((d for d in m.distributions() "
        "if d.metadata.get('Name','').lower().replace('_','-')==n),None) else None); "
        "print(json.dumps({'executable':sys.executable,'version':list(sys.version_info[:3]),"
        "'system':platform.system(),'machine':platform.machine(),"
        "'setuptools':get('setuptools'),'wheel':get('wheel')}))"
    )
    try:
        observation = json.loads(run([str(python_path), "-c", probe]))
    except (OSError, json.JSONDecodeError, subprocess.CalledProcessError):
        raise ValueError("selected build Python identity could not be observed") from None
    require(type(observation) is dict
            and type(observation.get("executable")) is str
            and Path(observation["executable"]).is_absolute()
            and type(observation.get("version")) is list
            and len(observation["version"]) == 3
            and all(type(part) is int and part >= 0 for part in observation["version"])
            and tuple(observation["version"][:2]) >= (3, 9)
            and type(observation.get("system")) is str
            and type(observation.get("machine")) is str,
            "selected build Python must be a supported, identified Python 3.9+ runtime")
    setuptools = observation.get("setuptools")
    wheel = observation.get("wheel")
    require(type(setuptools) is str and _version_tuple(setuptools) >= (77,)
            and type(wheel) is str and _version_tuple(wheel) >= (0,),
            "selected no-isolation builder requires setuptools>=77 and wheel")
    try:
        uv_version = run(["uv", "--version"])
    except (OSError, subprocess.CalledProcessError):
        raise ValueError("uv builder identity could not be observed") from None
    require(re.match(r"^uv \d+(?:\.\d+)+\b", uv_version) is not None,
            "uv builder version could not be identified")
    return {
        "mode": "selected-python-no-build-isolation",
        "python_path": str(python_path),
        "python_executable": observation["executable"],
        "python_version": ".".join(str(part) for part in observation["version"]),
        "platform_system": observation["system"],
        "platform_machine": observation["machine"],
        "setuptools_version": setuptools,
        "wheel_version": wheel,
        "uv_version": uv_version,
    }


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
    directory: Path, *, tag: str, repository: str, source_commit: str,
    build_python: Optional[Path] = None,
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    builder = selected_builder_identity(build_python) if build_python is not None else None
    command = ["uv", "build"]
    if build_python is not None:
        command.extend(["--python", str(build_python), "--no-build-isolation"])
    command.extend(["--out-dir", str(directory)])
    run(command)
    uv_ignore = directory / ".gitignore"
    if uv_ignore.exists():
        require(
            uv_ignore.is_file() and uv_ignore.read_text(encoding="utf-8") == "*",
            "unexpected file in temporary build output",
        )
        uv_ignore.unlink()
    return create_manifest(
        directory,
        directory / MANIFEST_NAME,
        tag=tag,
        repository=repository,
        source_commit=source_commit,
        package=PACKAGE,
        builder=builder,
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
        manifest.get("schema") == "github-release/1"
        and manifest.get("repository") == repository
        and manifest.get("tag") == tag
        and manifest.get("source_commit") == source_commit
        and manifest.get("package") == PACKAGE
        and manifest.get("version") == release_version(tag),
        "draft manifest source or package identity mismatch",
    )
    if "builder" in manifest:
        validate_builder_identity(manifest["builder"])
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
        "--build-python",
        type=Path,
        help="explicit executable Python path; opts into a validated no-build-isolation build",
    )
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
                    build_python=args.build_python,
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
