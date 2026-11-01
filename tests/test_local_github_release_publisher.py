import json
import sys
import tarfile
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from io import BytesIO
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from zipfile import ZipFile

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
import publish_github_release_local as publisher  # noqa: E402


class LocalPublisherTests(unittest.TestCase):
    def test_default_build_keeps_uv_isolation_and_has_no_selected_builder_record(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary) / "dist"
            manifest = {"schema": "github-release/1"}
            with patch.object(publisher, "run", return_value="") as run_build, \
                 patch.object(publisher, "create_manifest", return_value=manifest) as create:
                result = publisher.build_and_manifest(
                    directory, tag="v1.2.3", repository="example/ashlar",
                    source_commit="a" * 40)

        self.assertIs(result, manifest)
        self.assertEqual(run_build.call_args.args[0][:2], ["uv", "build"])
        self.assertNotIn("--no-build-isolation", run_build.call_args.args[0])
        self.assertNotIn("--python", run_build.call_args.args[0])
        self.assertIsNone(create.call_args.kwargs["builder"])

    def test_explicit_python_build_is_validated_and_recorded_in_manifest(self):
        with TemporaryDirectory() as temporary:
            python_path = Path(sys.executable)
            directory = Path(temporary) / "dist"
            observation = {
                "executable": str(python_path),
                "version": [3, 12, 8],
                "system": "Darwin",
                "machine": "arm64",
                "setuptools": "84.2.1",
                "wheel": "0.48.0",
            }
            commands = []

            def fake_run(command, **_kwargs):
                commands.append(command)
                if command[0] == str(python_path):
                    return json.dumps(observation)
                if command == ["uv", "--version"]:
                    return "uv 0.13.3 (test builder)"
                return ""

            with patch.object(publisher, "run", side_effect=fake_run), \
                 patch.object(publisher, "create_manifest", return_value={"ok": True}) as create:
                result = publisher.build_and_manifest(
                    directory, tag="v1.2.3", repository="example/ashlar",
                    source_commit="a" * 40, build_python=python_path)

        build_command = commands[-1]
        self.assertEqual(build_command,
                         ["uv", "build", "--python", str(python_path),
                          "--no-build-isolation", "--out-dir", str(directory)])
        identity = create.call_args.kwargs["builder"]
        self.assertEqual(identity["mode"], "selected-python-no-build-isolation")
        self.assertEqual(identity["python_version"], "3.12.8")
        self.assertEqual(identity["setuptools_version"], "84.2.1")
        self.assertEqual(identity["wheel_version"], "0.48.0")
        self.assertEqual(identity["uv_version"], "uv 0.13.3 (test builder)")
        self.assertEqual(result, {"ok": True})

    def test_explicit_python_requires_supported_python_and_build_backend(self):
        with TemporaryDirectory() as temporary:
            python_path = Path(sys.executable)
            refusals = (
                ({"version": [3, 8, 18], "setuptools": "84.2.1"}, r"Python 3.9\+"),
                ({"version": [3, 12, 8], "setuptools": "76.0.0"}, "setuptools>=77"),
            )
            for fields, message in refusals:
                observation = {
                    "executable": str(python_path), "version": fields["version"],
                    "system": "Darwin", "machine": "arm64",
                    "setuptools": fields["setuptools"], "wheel": "0.48.0",
                }
                with patch.object(publisher, "run", return_value=json.dumps(observation)):
                    with self.subTest(fields=fields), self.assertRaisesRegex(ValueError, message):
                        publisher.selected_builder_identity(python_path)

    def test_manifest_records_and_revalidates_selected_builder_identity(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary) / "dist"
            directory.mkdir()
            self.create_release_assets(directory)
            builder = {
                "mode": "selected-python-no-build-isolation",
                "python_path": "/private/tmp/build-venv/bin/python",
                "python_executable": "/private/tmp/build-venv/bin/python3.12",
                "python_version": "3.12.8",
                "platform_system": "Darwin",
                "platform_machine": "arm64",
                "setuptools_version": "84.2.1",
                "wheel_version": "0.48.0",
                "uv_version": "uv 0.13.3 (test builder)",
            }
            manifest_path = directory / publisher.MANIFEST_NAME
            manifest = publisher.create_manifest(
                directory, manifest_path, tag="v1.2.3", repository="example/ashlar",
                source_commit="a" * 40, package=publisher.PACKAGE, builder=builder)
            self.assertEqual(manifest["builder"], builder)

            assets = [
                {"name": path.name, "digest": f"sha256:{publisher.digest(path)}"}
                for path in sorted(directory.iterdir())
            ]
            release = Path(temporary) / "release.json"
            release.write_text(json.dumps({
                "tag_name": "v1.2.3", "draft": False, "prerelease": False,
                "immutable": True, "published_at": "2026-11-01T00:00:00Z",
                "assets": assets,
            }))
            publisher.verify_release(
                release, directory, tag="v1.2.3", repository="example/ashlar",
                source_commit="a" * 40, package=publisher.PACKAGE)

            invalid = dict(builder, python_version="3.8.18")
            with self.assertRaisesRegex(ValueError, "Python must be 3.9"):
                publisher.create_manifest(
                    directory, manifest_path, tag="v1.2.3", repository="example/ashlar",
                    source_commit="a" * 40, package=publisher.PACKAGE, builder=invalid)

    def test_draft_readback_rejects_invalid_optional_builder_identity(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary) / "release"
            directory.mkdir()
            self.create_release_assets(directory)
            manifest_path = directory / publisher.MANIFEST_NAME
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["builder"] = {"mode": "selected-python-no-build-isolation"}
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with patch.object(publisher, "run", return_value=""), \
                 patch.object(publisher, "verify_remote_assets"):
                with self.assertRaisesRegex(ValueError, "selected build identity is incomplete"):
                    publisher.download_and_verify_draft(
                        "v1.2.3", "example/ashlar", "a" * 40,
                        {"isDraft": True}, directory)

    def create_release_assets(self, directory):
        metadata = b"Metadata-Version: 2.1\nName: umf-ashlar\nVersion: 1.2.3\n\n"
        wheel = directory / "umf_ashlar-1.2.3-py3-none-any.whl"
        with ZipFile(wheel, "w") as archive:
            archive.writestr("umf_ashlar-1.2.3.dist-info/METADATA", metadata)
        sdist = directory / "umf_ashlar-1.2.3.tar.gz"
        info = tarfile.TarInfo("umf_ashlar-1.2.3/PKG-INFO")
        info.size = len(metadata)
        with tarfile.open(sdist, "w:gz") as archive:
            archive.addfile(info, BytesIO(metadata))
        publisher.create_manifest(
            directory,
            directory / publisher.MANIFEST_NAME,
            tag="v1.2.3",
            repository="example/ashlar",
            source_commit="a" * 40,
            package=publisher.PACKAGE,
        )
        return [
            {"name": path.name, "digest": f"sha256:{publisher.digest(path)}"}
            for path in sorted(directory.iterdir())
        ]

    def test_remote_asset_check_requires_exact_local_digests(self):
        with TemporaryDirectory() as temporary:
            directory = Path(temporary)
            metadata = b"Metadata-Version: 2.1\nName: umf-ashlar\nVersion: 1.2.3\n\n"
            wheel = directory / "umf_ashlar-1.2.3-py3-none-any.whl"
            with ZipFile(wheel, "w") as archive:
                archive.writestr("umf_ashlar-1.2.3.dist-info/METADATA", metadata)
            sdist = directory / "umf_ashlar-1.2.3.tar.gz"
            info = tarfile.TarInfo("umf_ashlar-1.2.3/PKG-INFO")
            info.size = len(metadata)
            with tarfile.open(sdist, "w:gz") as archive:
                archive.addfile(info, BytesIO(metadata))
            publisher.create_manifest(
                directory,
                directory / publisher.MANIFEST_NAME,
                tag="v1.2.3",
                repository="example/ashlar",
                source_commit="a" * 40,
                package=publisher.PACKAGE,
            )
            release = {
                "tagName": "v1.2.3",
                "assets": [
                    {"name": path.name, "digest": f"sha256:{publisher.digest(path)}"}
                    for path in sorted(directory.iterdir())
                ],
            }

            publisher.verify_remote_assets(release, directory)
            release["assets"][0]["digest"] = "sha256:" + "0" * 64
            with self.assertRaisesRegex(ValueError, "differ from locally verified build"):
                publisher.verify_remote_assets(release, directory)

    def test_publish_success_verifies_receipt_outside_distribution_directory(self):
        with TemporaryDirectory() as fixture_directory:
            fixture = Path(fixture_directory)
            assets = self.create_release_assets(fixture)
            draft = {"tagName": "v1.2.3", "isDraft": True, "assets": assets}
            published = {
                "tagName": "v1.2.3",
                "isDraft": False,
                "isPrerelease": False,
                "isImmutable": True,
                "publishedAt": "2026-10-31T12:00:00Z",
                "assets": assets,
            }
            argv = [
                "publish_github_release_local.py",
                "publish",
                "--tag",
                "v1.2.3",
                "--expected-commit",
                "a" * 40,
                "--repository",
                "example/ashlar",
                "--execute",
                "--confirm-publish",
                "v1.2.3",
            ]

            def fake_run(command, **_kwargs):
                if command[:3] == ["gh", "release", "download"]:
                    destination = Path(command[command.index("--dir") + 1])
                    for path in fixture.iterdir():
                        (destination / path.name).write_bytes(path.read_bytes())
                return ""

            with ExitStack() as stack:
                stack.enter_context(patch.object(sys, "argv", argv))
                stack.enter_context(patch.object(publisher, "check_source", return_value="a" * 40))
                stack.enter_context(patch.object(publisher, "check_repository_identity"))
                stack.enter_context(patch.object(publisher, "check_immutable_releases"))
                stack.enter_context(patch.object(publisher, "release_view", side_effect=[draft, published]))
                stack.enter_context(patch.object(publisher, "run", side_effect=fake_run))
                output = StringIO()
                with redirect_stdout(output):
                    self.assertEqual(publisher.main(), 0)

            self.assertTrue(json.loads(output.getvalue())["published"])

    def test_immutable_release_policy_must_be_enabled(self):
        with patch.object(
            publisher, "run", return_value='{"enabled": false, "enforced_by_owner": false}'
        ):
            with self.assertRaisesRegex(ValueError, "immutable releases must be enabled"):
                publisher.check_immutable_releases("example/ashlar")

    def test_stage_defaults_to_dry_run_without_mutating_github(self):
        commands = []
        argv = [
            "publish_github_release_local.py",
            "stage",
            "--tag",
            "v1.2.3",
            "--expected-commit",
            "a" * 40,
            "--repository",
            "example/ashlar",
        ]
        with ExitStack() as stack:
            stack.enter_context(patch.object(sys, "argv", argv))
            stack.enter_context(patch.object(publisher, "check_source", return_value="a" * 40))
            stack.enter_context(patch.object(publisher, "check_repository_identity"))
            stack.enter_context(patch.object(publisher, "check_immutable_releases"))
            stack.enter_context(patch.object(
                publisher,
                "build_and_manifest",
                return_value={"tag": "v1.2.3", "artifacts": []},
            ))
            stack.enter_context(patch.object(
                publisher,
                "run",
                side_effect=lambda command, **_kwargs: commands.append(command) or "",
            ))
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(publisher.main(), 0)

        summary = json.loads(output.getvalue())
        self.assertTrue(summary["dry_run"])
        self.assertEqual(summary["would_run"], "gh release create --verify-tag --draft")
        self.assertFalse(any(command[:3] == ["gh", "release", "create"] for command in commands))
        self.assertFalse(any(command[:3] == ["gh", "release", "edit"] for command in commands))

    def test_publish_requires_exact_confirmation_before_preflight(self):
        argv = [
            "publish_github_release_local.py",
            "publish",
            "--tag",
            "v1.2.3",
            "--expected-commit",
            "a" * 40,
            "--repository",
            "example/ashlar",
            "--execute",
        ]
        with ExitStack() as stack:
            stack.enter_context(patch.object(sys, "argv", argv))
            stack.enter_context(
                patch.object(publisher, "check_source", side_effect=AssertionError("source check ran"))
            )
            errors = StringIO()
            with redirect_stderr(errors):
                self.assertEqual(publisher.main(), 2)
        self.assertIn("--confirm-publish", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
