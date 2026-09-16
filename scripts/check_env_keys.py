"""Cross-check that every env var the Go code reads is provided by the k8s
manifests and by .env.example. Catches the classic drift where a manifest sets
a key the code never reads, or the code reads a key no manifest provides."""
import glob
import re
import sys

import yaml

# Every getEnv/getInt/getBool/getFloat/getDuration("KEY", ...) call site.
keys = set()
for path in glob.glob("internal/config/*.go"):
    src = open(path, encoding="utf-8").read()
    keys |= set(re.findall(r'get(?:Env|Int|Float|Bool|Duration)\("([A-Z0-9_]+)"', src))
    keys |= set(re.findall(r'os\.LookupEnv\("([A-Z0-9_]+)"', src))

print("code reads %d distinct env vars" % len(keys))

# Keys provided by the k8s configmap + secret.
k8s = {}
for f in glob.glob("deploy/k8s/0[12]-*.yaml"):
    for doc in yaml.safe_load_all(open(f, encoding="utf-8")):
        if not doc:
            continue
        kind = doc["kind"]
        for k in (doc.get("data") or {}).keys():
            k8s[k] = kind
        for k in (doc.get("stringData") or {}).keys():
            k8s[k] = kind

# Keys provided by .env.example.
example = set()
for line in open(".env.example", encoding="utf-8"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        example.add(line.split("=", 1)[0])

errors = 0

missing_k8s = sorted(k for k in keys if k not in k8s)
if missing_k8s:
    errors += 1
    print("\nFAIL: read by code but absent from k8s manifests:")
    for k in missing_k8s:
        print("  - %s" % k)

missing_example = sorted(k for k in keys if k not in example)
if missing_example:
    errors += 1
    print("\nFAIL: read by code but absent from .env.example:")
    for k in missing_example:
        print("  - %s" % k)

unused = sorted(k for k in k8s if k not in keys)
if unused:
    print("\nnote: set in k8s but not read by config (%d): %s" % (len(unused), ", ".join(unused)))

if errors:
    sys.exit(1)
print("\nPASS: config keys are consistent across code, k8s and .env.example")
