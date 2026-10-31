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
