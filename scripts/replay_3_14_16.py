"""Compatibility entry point for the revised read-only full-state validator.

No proof-gated migration is needed for normal active legacy VA.
"""
import argparse
import json
from pathlib import Path

from validate_va_snapshot import validate

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--migration-config-output', type=Path)
    args = parser.parse_args()
    result = validate(args.snapshot, None)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    if args.migration_config_output:
        config = json.loads((args.snapshot / 'config/symbols.json').read_text())
        for sym, cfg in config.items():
            saved = json.loads((args.snapshot / 'state' / (sym + '.json')).read_text())
            cfg['va_enabled'] = saved['va_enabled']
            cfg.pop('va_migration', None)
        args.migration_config_output.write_text(json.dumps(config, ensure_ascii=False, indent=2) + '\n')
    print('PASS: revised full-redacted-state differential validation (no broker connection)')
