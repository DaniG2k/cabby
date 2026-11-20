#!/usr/bin/env python3
"""Match a Cargo.lock against a RustSec advisory-db checkout (read-only, no cargo).
usage: python3 -I audit_lock.py <Cargo.lock> <advisory-db dir>"""
import re, sys, tomllib, pathlib

def parse_ver(v):
    v = v.split('+')[0]
    core, _, pre = v.partition('-')
    nums = [int(x) for x in core.split('.')]
    while len(nums) < 3: nums.append(0)
    return (tuple(nums), 0 if pre else 1, pre)

def cmp_req(ver, req):
    # all comma-separated comparators must hold
    for part in req.split(','):
        part = part.strip()
        m = re.match(r'^(>=|<=|>|<|=|\^|~)?\s*(.+)$', part)
        op, rv = m.group(1) or '^', m.group(2)
        v, r = parse_ver(ver), parse_ver(rv)
        if op == '>=': ok = v >= r
        elif op == '>': ok = v > r
        elif op == '<=': ok = v <= r
        elif op == '<': ok = v < r
        elif op == '=': ok = v[0] == r[0] and v[2] == r[2]
        elif op == '^':
            n = r[0]
            if n[0] > 0: hi = (n[0] + 1, 0, 0)
            elif n[1] > 0: hi = (0, n[1] + 1, 0)
            else: hi = (0, 0, n[2] + 1)
            ok = v >= r and v[0] < hi
        elif op == '~':
            n = r[0]; hi = (n[0], n[1] + 1, 0)
            ok = v >= r and v[0] < hi
        if not ok: return False
    return True

lock = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())
pkgs = {}
for p in lock['package']:
    if p.get('source', '').startswith('registry'):
        pkgs.setdefault(p['name'], set()).add(p['version'])
print(f"{sum(len(v) for v in pkgs.values())} registry packages in lock")
hits = []
for md in pathlib.Path(sys.argv[2], 'crates').glob('*/*.md'):
    name = md.parent.name
    if name not in pkgs: continue
    text = md.read_text()
    m = re.search(r'```toml\n(.*?)\n```', text, re.S)
    if not m: continue
    adv = tomllib.loads(m.group(1))
    a = adv['advisory']
    if a.get('withdrawn'): continue
    vers = adv.get('versions', {})
    patched, unaffected = vers.get('patched', []), vers.get('unaffected', [])
    for v in pkgs[name]:
        if any(cmp_req(v, r) for r in patched + unaffected): continue
        title = text.split('```', 2)[2].strip().splitlines()[0].lstrip('# ')
        hits.append((name, v, a['id'], a.get('informational', 'vulnerability'), a.get('date'), title))
for h in sorted(hits): print(' | '.join(str(x) for x in h))
print(f"{len(hits)} advisories match")
