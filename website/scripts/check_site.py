"""Check the production subpath, internal links, nav state and source seals."""
import argparse
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse, unquote

class Page(HTMLParser):
    def __init__(self, text):
        super().__init__()
        self.links, self.ids, self.current = [], set(), []
        self.seals = 0
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if "id" in attrs:
            self.ids.add(attrs["id"])
        if tag in ("a", "link") and "href" in attrs:
            self.links.append(attrs["href"])
        if tag == "a" and attrs.get("aria-current") == "page":
            self.current.append(attrs["href"])
        if tag == "script" and attrs.get("type") == "application/innsigle+json":
            self.seals += 1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--require-seals", action="store_true")
    args = parser.parse_args()
    root = args.root.resolve()
    expected = ["index.html"] + [f"{slug}/index.html" for slug in
                ("concepts", "schema", "background", "ecosystem", "start", "reference")]
    for relative in expected:
        path = root / relative
        page = Page(path.read_text())
        route = "/ashlar/" + ("" if relative == "index.html" else relative.removesuffix("index.html"))
        assert page.current and all(link == route for link in page.current), (relative, page.current)
        if args.require_seals:
            assert page.seals == 1, (relative, "missing/duplicate source attestation")
        for link in page.links:
            parsed = urlparse(link)
            if parsed.scheme or parsed.netloc:
                continue
            target = unquote(parsed.path)
            if target.startswith("/"):
                assert target.startswith("/ashlar/"), (relative, "escaped Pages subpath", link)
                destination = root / target.removeprefix("/ashlar/")
            elif target:
                destination = path.parent / target
            else:
                destination = path
            if destination.is_dir():
                destination /= "index.html"
            assert destination.exists(), (relative, "broken link", link)
            if parsed.fragment and destination.suffix == ".html":
                assert unquote(parsed.fragment) in Page(destination.read_text()).ids, (relative, link)
    if args.require_seals:
        assert (root / ".well-known/innsigle/keys.json").exists()
    print(f"PASS: {len(expected)} pages, internal links, current navigation" +
          (", source-seal coverage and issuer publication" if args.require_seals else ""))

if __name__ == "__main__":
    main()
