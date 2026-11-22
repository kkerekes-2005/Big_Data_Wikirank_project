import argparse
import csv
import heapq
import json
import os
import sys
import time
import zlib

COLS = ["Wikipedia_article_URL", "Language", "Page_ID", "Title",
        "WikiRank_score", "WikiRank_URL"]
TOP_K = 10
LIMITE_DURO = 2_000_000_000         
OBJETIVO_DEFECTO = 1_900_000_000   
MIN_DEFECTO = 10_000

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


def abrir_csv(ruta):
    return open(ruta, "r", encoding="utf-8", errors="replace", newline="")


def bytes_linea(row):
    """Bytes de la línea tal como la escribe csv.writer (QUOTE_MINIMAL, '\\n')."""
    n = len(row)
    for x in row:
        b = len(x.encode())
        if any(c in x for c in ',"\n\r'):
            b += 2 + x.count('"')
        n += b
    return n


def campos(row):
    """(lengua normalizada, Page_ID, puntuación) para el muestreo y el top 10."""
    r = (row + [""] * 6)[:6]
    return r[1].strip().lower(), r[2].strip(), r[4].strip()


def clave_orden(score_str, pid_str):
    """Clave para el top 10: mayor puntuación y, a igualdad, menor Page_ID."""
    try:
        return (float(score_str), -int(pid_str))
    except ValueError:
        return None


def u_hash(semilla, lang, pid):
    """Número reproducible en [0, 1). Igual que F.crc32 en Spark / 2^32."""
    return zlib.crc32(f"{semilla}|{lang}|{pid}".encode()) / 4294967296.0


def pasada1(ruta, semilla, minimo, informe_cada=5_000_000):
    stats, tops, menores = {}, {}, {}
    filas = malformadas = 0
    t0 = time.time()
    with abrir_csv(ruta) as f:
        reader = csv.reader(f)
        cabecera = next(reader)
        if [c.strip() for c in cabecera] != COLS:
            print("AVISO: cabecera inesperada:", cabecera, file=sys.stderr)
        for row in reader:
            filas += 1
            if len(row) != 6:
                malformadas += 1
            ln, pid, score = campos(row)
            st = stats.get(ln)
            if st is None:
                st = stats[ln] = {"n": 0, "bytes": 0, "suma": 0.0, "n_validas": 0}
                tops[ln], menores[ln] = [], []
            st["n"] += 1
            st["bytes"] += bytes_linea(row)
            k = clave_orden(score, pid)
            if k is not None:
                if 0 <= k[0] <= 100:
                    st["suma"] += k[0]
                    st["n_validas"] += 1
                h = tops[ln]
                if len(h) < TOP_K:
                    heapq.heappush(h, k)
                elif k > h[0]:
                    heapq.heapreplace(h, k)
            if minimo:
                u = u_hash(semilla, ln, pid)
                m = menores[ln]
                if len(m) < minimo:
                    heapq.heappush(m, -u)
                elif u < -m[0]:
                    heapq.heapreplace(m, -u)
            if filas % informe_cada == 0:
                print(f"  pasada 1: {filas:,} filas ({time.time() - t0:.0f} s)", flush=True)

    umbral_top = {ln: (min(h) if len(h) == TOP_K else (float("-inf"), 0)) for ln, h in tops.items()}
    umbral_min = {}
    for ln, m in menores.items():
        umbral_min[ln] = 1.0 if (not minimo or stats[ln]["n"] <= minimo) else -m[0]
    return {"stats": stats, "umbral_top": umbral_top, "umbral_min": umbral_min,
            "cab_bytes": bytes_linea(COLS), "filas": filas, "malformadas": malformadas,
            "segundos": time.time() - t0}


def fracciones(p1, f):
    """Fracción efectiva de cada lengua: la común o la que garantiza el mínimo."""
    return {ln: max(f, p1["umbral_min"][ln]) for ln in p1["stats"]}


def bytes_esperados(p1, f):
    fl = fracciones(p1, f)
    return p1["cab_bytes"] + sum(st["bytes"] * min(1.0, fl[ln] if fl[ln] < 1.0 else 1.0)
                                 for ln, st in p1["stats"].items())


def calcular_fraccion(p1, objetivo):
    """Mayor f común tal que el tamaño esperado (con mínimos) no supere el objetivo."""
    meta = objetivo * 0.995            
    if bytes_esperados(p1, 1.0) <= meta:
        return 1.0
    lo, hi = 0.0, 1.0
    for _ in range(60):              
        mid = (lo + hi) / 2
        if bytes_esperados(p1, mid) <= meta:
            lo = mid
        else:
            hi = mid
    if lo == 0.0:
        sys.exit("ERROR: con ese mínimo por lengua no se puede llegar al tamaño objetivo")
    return lo


def pasada2(ruta_in, ruta_out, p1, f, fl, semilla, informe_cada=5_000_000):
    umbral_top = p1["umbral_top"]
    sel = {ln: {"n": 0, "forzadas": 0} for ln in p1["stats"]}
    filas = 0
    t0 = time.time()
    tmp = ruta_out + ".tmp"
    with abrir_csv(ruta_in) as fin, open(tmp, "w", encoding="utf-8", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n")
        writer.writerow(next(reader))    
        for row in reader:
            filas += 1
            ln, pid, score = campos(row)
            k = clave_orden(score, pid)
            forzada = k is not None and k >= umbral_top[ln]
            if fl[ln] >= 1.0 or forzada or u_hash(semilla, ln, pid) <= fl[ln]:
                writer.writerow(row)    
                sel[ln]["n"] += 1
                if forzada:
                    sel[ln]["forzadas"] += 1
            if filas % informe_cada == 0:
                print(f"  pasada 2: {filas:,} filas ({time.time() - t0:.0f} s)", flush=True)
    os.replace(tmp, ruta_out) 
    return sel, time.time() - t0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--entrada", required=True, help="wikirank_20240801.csv original")
    ap.add_argument("--salida", required=True, help="CSV del subconjunto")
    ap.add_argument("--metadatos", help="JSON de metadatos (por defecto: <salida>_metadata.json)")
    ap.add_argument("--max-bytes", type=int, default=OBJETIVO_DEFECTO,
                    help=f"tamaño objetivo del CSV (por defecto {OBJETIVO_DEFECTO:,})")
    ap.add_argument("--min-por-lengua", type=int, default=MIN_DEFECTO,
                    help=f"filas mínimas garantizadas por lengua (por defecto {MIN_DEFECTO:,}; 0 = sin mínimo)")
    ap.add_argument("--semilla", default="42", help="sal del hash de muestreo")
    a = ap.parse_args()

    if a.max_bytes > LIMITE_DURO:
        sys.exit(f"--max-bytes no puede superar {LIMITE_DURO:,} (límite del enunciado)")
    meta_path = a.metadatos or os.path.splitext(a.salida)[0] + "_metadata.json"

    print(f"Pasada 1 sobre {a.entrada} ({os.path.getsize(a.entrada):,} bytes)")
    p1 = pasada1(a.entrada, a.semilla, a.min_por_lengua)
    print(f"  {p1['filas']:,} filas, {len(p1['stats'])} lenguas, "
          f"{p1['malformadas']} malformadas, {p1['segundos']:.0f} s")

    f = calcular_fraccion(p1, a.max_bytes)
    fl = fracciones(p1, f)
    print(f"Fracción común: {f:.6f}" + ("  (no hace falta muestrear)" if f >= 1 else ""))
    for ln in sorted(fl):
        if fl[ln] > f:
            print(f"  {ln}: fracción {fl[ln]:.4f} para garantizar el mínimo "
                  f"({min(p1['stats'][ln]['n'], a.min_por_lengua):,} filas)")

    print(f"Pasada 2: escribiendo {a.salida}")
    sel, seg2 = pasada2(a.entrada, a.salida, p1, f, fl, a.semilla)
    tam = os.path.getsize(a.salida)
    print(f"  {sum(s['n'] for s in sel.values()):,} filas, {tam:,} bytes, {seg2:.0f} s")

    lenguas = {}
    for ln, st in sorted(p1["stats"].items()):
        lenguas[ln] = {
            "n_original": st["n"],
            "media_original": round(st["suma"] / st["n_validas"], 3) if st["n_validas"] else None,
            "fraccion_lengua": min(1.0, fl[ln]),
            "n_seleccion": sel[ln]["n"],
            "top10_forzadas": sel[ln]["forzadas"],
        }
    corto = [ln for ln, v in lenguas.items() if v["n_seleccion"] < min(v["n_original"], a.min_por_lengua)]
    meta = {
        "fichero_original": os.path.basename(a.entrada),
        "bytes_original": os.path.getsize(a.entrada),
        "filas_original": p1["filas"],
        "fichero_seleccion": os.path.basename(a.salida),
        "bytes_seleccion": tam,
        "filas_seleccion": sum(s["n"] for s in sel.values()),
        "columnas": COLS,
        "fraccion": f,
        "min_por_lengua": a.min_por_lengua,
        "semilla_hash": a.semilla,
        "top_k_forzado": TOP_K,
        "filas_malformadas": p1["malformadas"],
        "lenguas": lenguas,
    }
    with open(meta_path, "w", encoding="utf-8") as fm:
        json.dump(meta, fm, ensure_ascii=False, indent=2)
    print(f"Metadatos en {meta_path}")

    if corto:
        sys.exit(f"ERROR: lenguas por debajo del mínimo: {corto}")
    if tam > LIMITE_DURO:
        sys.exit(f"ERROR: el subconjunto ocupa {tam:,} bytes (> 2 GB). "
                 f"Vuelve a ejecutar con un --max-bytes menor.")
    print(f"OK: subconjunto dentro del límite de 2 GB y con al menos "
          f"{a.min_por_lengua:,} filas por lengua (o la lengua entera).")


if __name__ == "__main__":
    main()
