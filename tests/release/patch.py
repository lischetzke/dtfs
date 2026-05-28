import re, sys
p = sys.argv[1]
s = open(p).read()
new = re.sub(r"(disks: Vec::new\(\),\n)(        \}\))", r"\1            quarantine: None,\n\2", s)
n = s.count("disks: Vec::new(),\n        })")
print(f"changes: {n}")
open(p, "w").write(new)
