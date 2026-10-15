import io
import sys

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")

current = None
count = 0
total = 0.0


def emit(lang, n, s):
    if n > 0:
        print("%s\t%d\t%.3f" % (lang, n, s / n))


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 2:
        continue

    lang, value_str = parts[0].strip(), parts[1].strip()

    try:
        value = float(value_str)
    except ValueError:
        continue

    if lang != current:
        if current is not None:
            emit(current, count, total)
        current = lang
        count = 0
        total = 0.0

    count += 1
    total += value

if current is not None:
    emit(current, count, total)