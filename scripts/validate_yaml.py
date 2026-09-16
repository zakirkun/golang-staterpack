import sys, glob
import yaml

ok = True
for f in sorted(glob.glob('deploy/k8s/*.yaml')):
    try:
        docs = [d for d in yaml.safe_load_all(open(f, encoding='utf-8')) if d]
        kinds = ", ".join(d.get("kind", "?") for d in docs)
        print("OK  %-32s %s" % (f, kinds))
    except Exception as e:
        ok = False
        print("BAD %-32s %s" % (f, e))
sys.exit(0 if ok else 1)
