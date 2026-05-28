import argparse
import ast
import os
from pathlib import Path

import pandas as pd


EXPECTED_DBS = {"postgres", "mariadb", "mongodb", "cassandra"}
EXPECTED_SIZES = {10000, 100000, 1000000}
EXPECTED_OPERATIONS = {"CREATE", "READ", "UPDATE", "DELETE"}
EXPECTED_SCENARIOS_PER_OPERATION = 3


class Progress:
    def __init__(self, total_steps: int, label: str) -> None:
        self.total_steps = max(total_steps, 1)
        self.label = label
        self.done_steps = 0
        self.started_at = pd.Timestamp.now()

    def step(self, message: str) -> None:
        self.done_steps = min(self.done_steps + 1, self.total_steps)
        elapsed = (pd.Timestamp.now() - self.started_at).total_seconds()
        eta = self._eta(elapsed)
        print(
            f"[INFO] {self.label}: {self.done_steps}/{self.total_steps} "
            f"({self.done_steps / self.total_steps * 100:.1f}%) | ETA {eta} | {message}",
            flush=True,
        )

    def _eta(self, elapsed: float) -> str:
        if self.done_steps <= 0:
            return "liczenie..."
        remaining = elapsed / self.done_steps * (self.total_steps - self.done_steps)
        return format_duration(remaining)


def format_duration(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m {sec}s"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def validate_results(df: pd.DataFrame) -> list[str]:
    warnings: list[str] = []
    required_cols = {
        "db",
        "size",
        "operation",
        "scenario",
        "avg_ms",
        "throughput_rec_s",
        "samples_ms",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Brak wymaganych kolumn: {', '.join(sorted(missing))}")

    dbs = set(df["db"].dropna().unique())
    sizes = set(df["size"].dropna().astype(int).unique())
    operations = set(df["operation"].dropna().unique())

    if dbs != EXPECTED_DBS:
        warnings.append(f"Bazy w wynikach: {sorted(dbs)}; oczekiwano: {sorted(EXPECTED_DBS)}")
    if sizes != EXPECTED_SIZES:
        warnings.append(f"Rozmiary w wynikach: {sorted(sizes)}; oczekiwano: {sorted(EXPECTED_SIZES)}")
    if operations != EXPECTED_OPERATIONS:
        warnings.append(f"Operacje w wynikach: {sorted(operations)}; oczekiwano: {sorted(EXPECTED_OPERATIONS)}")

    expected_rows = len(EXPECTED_DBS) * len(EXPECTED_SIZES) * len(EXPECTED_OPERATIONS) * EXPECTED_SCENARIOS_PER_OPERATION
    if len(df) != expected_rows:
        warnings.append(f"Liczba wierszy wynikowych: {len(df)}; oczekiwano: {expected_rows}")

    scenario_counts = df.groupby(["db", "size", "operation"])["scenario"].nunique()
    incomplete = scenario_counts[scenario_counts != EXPECTED_SCENARIOS_PER_OPERATION]
    if not incomplete.empty:
        warnings.append("Nie kazda kombinacja baza/rozmiar/operacja ma 3 scenariusze.")

    sample_lengths: list[int] = []
    for value in df["samples_ms"]:
        try:
            parsed = ast.literal_eval(str(value))
        except Exception:
            parsed = []
        sample_lengths.append(len(parsed) if isinstance(parsed, list) else 0)
    if any(length != 3 for length in sample_lengths):
        warnings.append("Nie kazdy wiersz wynikowy zawiera 3 proby w kolumnie samples_ms.")

    return warnings


def save_bar_chart(data: pd.DataFrame, x: str, y: str, hue: str, title: str, output_path: Path) -> None:
    pivot = data.pivot(index=x, columns=hue, values=y).fillna(0)
    categories = [str(v) for v in pivot.index.tolist()]
    series = [str(v) for v in pivot.columns.tolist()]
    colors = ["#3366cc", "#dc3912", "#109618", "#ff9900", "#990099", "#0099c6"]

    width = 1000
    height = 620
    margin_left = 90
    margin_right = 40
    margin_top = 70
    margin_bottom = 130
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    max_value = float(pivot.max().max()) or 1.0
    group_width = plot_width / max(len(categories), 1)
    bar_width = group_width / (len(series) + 1)

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{width / 2}" y="34" text-anchor="middle" font-family="Arial" font-size="24" font-weight="700">{escape_svg(title)}</text>',
    ]

    for i in range(6):
        value = max_value * i / 5
        y_pos = margin_top + plot_height - (value / max_value * plot_height)
        svg.append(f'<line x1="{margin_left}" y1="{y_pos:.1f}" x2="{width - margin_right}" y2="{y_pos:.1f}" stroke="#e5e7eb"/>')
        svg.append(
            f'<text x="{margin_left - 10}" y="{y_pos + 4:.1f}" text-anchor="end" font-family="Arial" font-size="12" fill="#374151">{value:.1f}</text>'
        )

    for cat_idx, category in enumerate(categories):
        group_x = margin_left + cat_idx * group_width
        label_x = group_x + group_width / 2
        svg.append(
            f'<text x="{label_x:.1f}" y="{height - 82}" text-anchor="middle" font-family="Arial" font-size="13" fill="#111827">{escape_svg(category)}</text>'
        )
        for series_idx, serie in enumerate(series):
            value = float(pivot.loc[pivot.index[cat_idx], serie])
            bar_height = value / max_value * plot_height
            x_pos = group_x + series_idx * bar_width + bar_width * 0.5
            y_pos = margin_top + plot_height - bar_height
            svg.append(
                f'<rect x="{x_pos:.1f}" y="{y_pos:.1f}" width="{bar_width * 0.82:.1f}" height="{bar_height:.1f}" fill="{colors[series_idx % len(colors)]}"/>'
            )

    legend_x = margin_left
    legend_y = height - 44
    for idx, serie in enumerate(series):
        item_x = legend_x + idx * 150
        svg.append(f'<rect x="{item_x}" y="{legend_y}" width="14" height="14" fill="{colors[idx % len(colors)]}"/>')
        svg.append(
            f'<text x="{item_x + 20}" y="{legend_y + 12}" font-family="Arial" font-size="13" fill="#111827">{escape_svg(serie)}</text>'
        )

    svg.append(f'<text x="{margin_left + plot_width / 2}" y="{height - 16}" text-anchor="middle" font-family="Arial" font-size="13" fill="#374151">{escape_svg(x)}</text>')
    svg.append(f'<text x="22" y="{margin_top + plot_height / 2}" transform="rotate(-90 22 {margin_top + plot_height / 2})" text-anchor="middle" font-family="Arial" font-size="13" fill="#374151">{escape_svg(y)}</text>')
    svg.append("</svg>")
    output_path.write_text("\n".join(svg), encoding="utf-8")


def escape_svg(value: object) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def write_markdown_summary(df: pd.DataFrame, warnings: list[str], output_path: Path) -> None:
    operation_summary = (
        df.groupby(["db", "operation"], as_index=False)["avg_ms"]
        .mean()
        .sort_values(["operation", "avg_ms"])
    )
    size_summary = (
        df.groupby(["db", "size"], as_index=False)["avg_ms"]
        .mean()
        .sort_values(["size", "avg_ms"])
    )

    lines = [
        "# Podsumowanie wynikow CRUD",
        "",
        "Plik wygenerowany automatycznie na podstawie `results/crud_benchmarks.csv`.",
        "",
    ]
    if warnings:
        lines.extend(["## Ostrzezenia walidacyjne", ""])
        lines.extend([f"- {warning}" for warning in warnings])
        lines.append("")

    lines.extend(
        [
            "## Sredni czas wedlug operacji",
            "",
            markdown_table(operation_summary),
            "",
            "## Sredni czas wedlug rozmiaru zbioru",
            "",
            markdown_table(size_summary),
            "",
            "## Wykresy",
            "",
            "- `charts/avg_ms_by_operation_db.svg`",
            "- `charts/avg_ms_by_size_db.svg`",
            "- `charts/throughput_by_operation_db.svg`",
            "",
        ]
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")


def markdown_table(df: pd.DataFrame) -> str:
    headers = [str(col) for col in df.columns]
    rows = [[str(value) for value in row] for row in df.to_numpy()]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def generate_assets(input_csv: str, output_dir: str) -> None:
    progress = Progress(8, "Assets")
    print(f"[INFO] Reading benchmark results from {input_csv}", flush=True)
    df = pd.read_csv(input_csv)
    progress.step("CSV loaded")

    print("[INFO] Validating benchmark result shape", flush=True)
    warnings = validate_results(df)
    progress.step("Validation completed")

    out_dir = Path(output_dir)
    charts_dir = out_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    progress.step(f"Output directories ready: {out_dir}")

    print("[INFO] Aggregating results by operation, size and throughput", flush=True)
    by_operation = df.groupby(["operation", "db"], as_index=False)["avg_ms"].mean()
    by_size = df.groupby(["size", "db"], as_index=False)["avg_ms"].mean()
    throughput = df.groupby(["operation", "db"], as_index=False)["throughput_rec_s"].mean()
    progress.step("Aggregations prepared")

    print("[INFO] Writing chart avg_ms_by_operation_db.svg", flush=True)
    save_bar_chart(
        by_operation,
        x="operation",
        y="avg_ms",
        hue="db",
        title="Sredni czas operacji CRUD wedlug bazy",
        output_path=charts_dir / "avg_ms_by_operation_db.svg",
    )
    progress.step("Operation chart saved")

    print("[INFO] Writing chart avg_ms_by_size_db.svg", flush=True)
    save_bar_chart(
        by_size,
        x="size",
        y="avg_ms",
        hue="db",
        title="Sredni czas testow wedlug rozmiaru zbioru",
        output_path=charts_dir / "avg_ms_by_size_db.svg",
    )
    progress.step("Size chart saved")

    print("[INFO] Writing chart throughput_by_operation_db.svg", flush=True)
    save_bar_chart(
        throughput,
        x="operation",
        y="throughput_rec_s",
        hue="db",
        title="Srednia przepustowosc wedlug operacji CRUD",
        output_path=charts_dir / "throughput_by_operation_db.svg",
    )
    progress.step("Throughput chart saved")

    df.groupby(["db", "size", "operation"], as_index=False)["avg_ms"].mean().to_csv(
        out_dir / "crud_summary.csv",
        index=False,
    )
    write_markdown_summary(df, warnings, out_dir / "wyniki_podsumowanie.md")
    progress.step("Summary CSV and markdown saved")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate charts and markdown summary from CRUD benchmark CSV")
    parser.add_argument("--input", default="results/crud_benchmarks.csv")
    parser.add_argument("--output-dir", default="reports/assets")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Nie znaleziono pliku wynikow: {args.input}")

    generate_assets(args.input, args.output_dir)
    print(f"[INFO] Assets generated in: {args.output_dir}")


if __name__ == "__main__":
    main()
