"""Project-local AST import boundary check; no imports of inspected modules."""
from __future__ import annotations
import argparse
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / 'tools/module_boundaries.json'
VENDORS = {'pyspark', 'delta', 'databricks', 'psycopg', 'psycopg2', 'gremlin_python', 'neo4j', 'duckdb', 'pyarrow', 'opentelemetry'}


def imports(root: Path):
    paths = sorted(list((root / 'src/ashlar').rglob('*.py')) + list((root / 'src/ashlar_host').rglob('*.py')) + list((root / 'tools').rglob('*.py')))
    for path in paths:
        source = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(), filename=source)
        aliases = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for name in node.names:
                    aliases[name.asname or name.name.split('.')[0]] = name.name if name.asname else name.name.split('.')[0]
                    yield source, name.name, '*', node.lineno
            elif isinstance(node, ast.ImportFrom):
                target = node.module or ''
                if node.level and source.startswith(('src/ashlar/', 'src/ashlar_host/')):
                    package_name = source.split('/')[1]
                    package = [package_name] + list(path.relative_to(root / 'src' / package_name).parts[:-1])
                    target = '.'.join(package[:len(package) - node.level + 1] + ([target] if target else []))
                for name in node.names:
                    if target in {'ashlar', 'ashlar_host'}:
                        aliases[name.asname or name.name] = target + '.' + name.name
                    selected = target + '.' + name.name
                    if target.startswith('ashlar') and (root / 'src' / Path(*selected.split('.'))).with_suffix('.py').exists():
                        aliases[name.asname or name.name] = selected
                        yield source, selected, '*', node.lineno
                    else:
                        yield source, target, name.name, node.lineno
        def resolve(node):
            if isinstance(node, ast.Name):
                return aliases.get(node.id)
            if isinstance(node, ast.Attribute):
                prefix = resolve(node.value)
                return prefix + '.' + node.attr if prefix else None
            return None
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                target = resolve(node.value)
                if target:
                    yield source, target, node.attr, node.lineno


def violation(source: str, target: str, symbol: str):
    if source.startswith('src/ashlar/'):
        stem = Path(source).stem
        if target == 'ashlar_host' or target.startswith('ashlar_host.'):
            if source not in {'src/ashlar/cli.py', 'src/ashlar/__main__.py'}:
                return 'core-to-host-composition'
        host = {'cli', 'source_config', 'weft_distribution', 'weft_paths_distribution', 'weft_paths_keys_distribution', 'commerce_source', 'supply_chain_source', 'archaeology_source', 'ecology_source', 'medical_source', '__main__'}
        model = {'schema', 'catalog', 'binding', 'semantic_policy', 'typed_source_policy'}
        source_state = {'source', 'apply', 'whole_entity', 'source_checkpoint'}
        transport = {'native', 'staging', 'attempt_store', 'schema_registry', 'pins', 'authority', 'retention_policy'}
        consumer = {'weft_installation', 'weft_paths_package', 'weft_paths_installation', 'weft_paths_keys_package', 'weft_paths_keys_installation', '_weft_installation_mechanics', 'weft_binding', 'weft_query', 'weft_decode', 'weft_path_decode', 'graph_release', 'singleton'}
        producer = {'publisher', 'stored_publisher', 'durable_publisher', 'outbox'}
        selected = target.split('.')[-1] if target.startswith('ashlar.') else ''
        if stem in model | source_state and selected in transport | host:
            return 'portable-to-runtime'
        if stem in consumer and selected in producer:
            return 'consumer-to-producer'
        if stem not in host and target.startswith('ashlar.') and target.split('.')[-1] in host:
            return 'core-to-composition'
        if target == 'tools' or target.startswith('tools.'):
            return 'core-to-tools'
        if target.split('.')[0] in VENDORS:
            return 'core-to-sdk'
    if source.startswith('src/ashlar_host/') and (target == 'tools' or target.startswith('tools.')):
        return 'host-to-checkout-tools'
    if (target in {'ashlar', 'ashlar_host'} or target.startswith(('ashlar.', 'ashlar_host.'))) and symbol.startswith('_'):
        own = '.'.join(Path(source).with_suffix('').parts[1:]) if source.startswith(('src/ashlar/', 'src/ashlar_host/')) else ''
        if own.endswith('.__init__'):
            own = own[:-9]
        if target != own:
            return 'private-cross-module'
    return None


def scan(root: Path):
    return sorted({(source, target, symbol, reason) for source, target, symbol, _ in imports(root)
                   if (reason := violation(source, target, symbol))})


def check(root: Path, policy: dict):
    if set(policy) != {'format', 'limitations', 'baseline'} or policy['format'] != 'ashlar-boundary-baseline/0.1':
        raise ValueError('closed boundary policy required')
    accepted = set()
    for entry in policy['baseline']:
        if set(entry) != {'source', 'target', 'symbol', 'reason', 'owner', 'removal_trigger'} or not all(isinstance(x, str) and x for x in entry.values()):
            raise ValueError('closed individually owned baseline required')
        key = tuple(entry[x] for x in ('source', 'target', 'symbol', 'reason'))
        if key in accepted:
            raise ValueError('duplicate baseline')
        accepted.add(key)
    actual = set(scan(root))
    return sorted(actual - accepted), sorted(accepted - actual)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--policy', type=Path, default=POLICY)
    args = parser.parse_args()
    new, stale = check(args.root, json.loads(args.policy.read_text()))
    if new or stale:
        for entry in new:
            print('FORBIDDEN', *entry)
        for entry in stale:
            print('STALE_BASELINE', *entry)
        return 1
    print('Module import boundaries pass; dynamic imports/type ownership/cycles require semantic review.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
