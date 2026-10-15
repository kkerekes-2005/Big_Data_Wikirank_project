import csv
import io
import sys

sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")

for row in csv.reader(sys.stdin):
    if len(row) != 6 or row[0] == "Wikipedia_article_URL":   
        continue
    lang, score = row[1].strip().lower(), row[4]
    try:
        value = float(score)
    except ValueError:
        continue
    if not lang or not (0 <= value <= 100):   # mismos filtros que la limpieza de Spark
        continue
    print("%s\t%s" % (lang, value))