#!/usr/bin/env python3
"""Read-only HEAD preflight for the four frozen Fabric OneLake candidates.

The Azure CLI token is captured only in process memory. This script makes no
requests outside the hard-coded HTTPS host/path allowlist and refuses redirects.
It prints only candidate labels, HTTP status, and a sanitized storage error code.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from urllib.parse import urlsplit


HOST = "onelake.dfs.fabric.microsoft.com"
WORKSPACE = "0a4dbd9c-e1a7-4ec8-8ce9-aa51991ff793"
LAKEHOUSE = "ee66f2e8-9072-4a48-8364-ecd2f2967c3e"
PATHS = (
    (
        "R1-nodes",
        "Tables/dbo/ashlar_commerce_r1_70cca0aee0db431c81e96651a43f74e7_nodes",
    ),
    (
        "R1-edges",
        "Tables/dbo/ashlar_commerce_r1_70cca0aee0db431c81e96651a43f74e7_edges",
    ),
    (
        "R2-nodes",
        "Tables/dbo/ashlar_commerce_r2_4d559fd9ac44462bb20855826d9ad35a_nodes",
    ),
    (
        "R2-edges",
        "Tables/dbo/ashlar_commerce_r2_4d559fd9ac44462bb20855826d9ad35a_edges",
    ),
)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def get_storage_token() -> str:
    completed = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "https://storage.azure.com/",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        shell=False,
    )
    token = completed.stdout.strip()
    if completed.returncode != 0 or not token:
        raise RuntimeError("azure-cli-token-unavailable")
    return token


def candidate_url(relative_path: str) -> str:
    url = (
        f"https://{HOST}/{WORKSPACE}/{LAKEHOUSE}/{relative_path}"
    )
    parsed = urlsplit(url)
    expected_path = f"/{WORKSPACE}/{LAKEHOUSE}/{relative_path}"
    if (
        parsed.scheme != "https"
        or parsed.hostname != HOST
        or parsed.port is not None
        or parsed.path != expected_path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("candidate-outside-allowlist")
    return url


def head_status(url: str, token: str) -> tuple[int | None, str | None]:
    request = urllib.request.Request(
        url,
        method="HEAD",
        headers={"Authorization": f"Bearer {token}"},
    )
    opener = urllib.request.build_opener(NoRedirect())
    try:
        with opener.open(request, timeout=30) as response:
            return response.status, sanitize_error_code(
                response.headers.get("x-ms-error-code")
            )
    except urllib.error.HTTPError as error:
        # Includes 3xx responses rejected by NoRedirect. Do not print headers.
        return error.code, sanitize_error_code(
            error.headers.get("x-ms-error-code") if error.headers else None
        )
    except Exception as error:  # Emit the class only; exception text can include URLs.
        return None, type(error).__name__


def sanitize_error_code(value: str | None) -> str | None:
    if value is None:
        return None
    match = re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value)
    return match.group(0) if match else "unrecognized-code"


def main() -> int:
    allowlist = {
        candidate_url(relative_path): label for label, relative_path in PATHS
    }
    try:
        token = get_storage_token()
    except Exception as error:
        # Never surface CLI stderr/stdout or exception details.
        print(json.dumps({"status": "blocked", "reason": type(error).__name__}))
        return 2

    observations = []
    for url, label in allowlist.items():
        parsed = urlsplit(url)
        if parsed.hostname != HOST or url not in allowlist:
            print(json.dumps({"status": "blocked", "reason": "allowlist-check"}))
            return 3
        status, error_code = head_status(url, token)
        observations.append(
            {"label": label, "status": status, "storage_error_code": error_code}
        )
    # Drop the reference before emitting output; no token or response headers appear.
    token = ""
    print(json.dumps({"method": "HEAD", "observations": observations}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
