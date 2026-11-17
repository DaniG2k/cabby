#!/usr/bin/env python3
"""Rebuild the [[test]] entries of tests/Cargo.toml from their name/path pairs.

Union merges of parallel translation branches can drop or duplicate the shared
`[[test]]` header lines. This keeps everything before the first entry and
rewrites the entries, de-duplicated, in their original order.
"""
import re, sys, pathlib

p = pathlib.Path(__file__).resolve().parents[2] / "tests" / "Cargo.toml"
text = p.read_text()
head, sep, body = text.partition("\n[[test]]")
if not sep:
    sys.exit("no [[test]] entries found")
pairs = re.findall(r'^name = "([^"]+)"\s*\npath = "([^"]+)"', sep + body, re.M)
seen, entries = set(), []
for name, path in pairs:
    if name in seen:
        continue
    seen.add(name)
    entries.append(f'[[test]]\nname = "{name}"\npath = "{path}"\n')
p.write_text(head.rstrip("\n") + "\n\n" + "\n".join(entries))
print(f"{len(entries)} test targets")
