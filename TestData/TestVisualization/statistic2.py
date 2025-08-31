#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Optional, List, Tuple
import textwrap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import re

def load_elapsed_only(csv_path: Path, dataset_label: str) -> pd.DataFrame:
    """
    Load a CSV and return rows with:
      - elapsed (ms)          -> numeric
      - label                 -> dataset label from CLI (for grouping/ordering)
      - users_factor          -> int parsed from FIRST ROW of CSV 'label' column
                                 (e.g., '5 Users - ...' -> 5). If missing/not found/0 => 1.
      - error                 -> bool, True if responseCode >= 400
    NOTE: We align 'error' to the same rows that survive elapsed dropna.
    """
    df = pd.read_csv(csv_path)

    if "elapsed" not in df.columns:
        raise ValueError(
            f"File '{csv_path}' must contain an 'elapsed' column. Found columns: {list(df.columns)}"
        )

    elapsed = pd.to_numeric(df["elapsed"], errors="coerce")
    mask = elapsed.notna()

    rc_col = next((c for c in df.columns if c.lower() == "responsecode"), None)
    if rc_col is not None:
        rc = pd.to_numeric(df[rc_col], errors="coerce")
        err = (rc >= 400).fillna(False)
    else:
        err = pd.Series(False, index=df.index)

    csv_label_col = next((c for c in df.columns if c.lower() == "label"), None)
    users_factor = 1
    if csv_label_col is not None and len(df) > 0:
        first_lbl_text = str(df.iloc[0][csv_label_col])
        m = re.search(r"\b(\d+)\s*user[s]?\b", first_lbl_text, flags=re.IGNORECASE)
        if m is None:
            nums = re.findall(r"\d+", first_lbl_text)
            if nums:
                try:
                    users_factor = max(1, int(nums[-1]))
                except Exception:
                    users_factor = 1
        else:
            try:
                users_factor = max(1, int(m.group(1)))
            except Exception:
                users_factor = 1

    out = pd.DataFrame({
        "elapsed": elapsed[mask],
        "error": err[mask]
    })
    out.reset_index(drop=True, inplace=True)
    out["label"] = dataset_label
    out["users_factor"] = users_factor
    return out


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    def p(series, q): return np.percentile(series, q)

    out = df.groupby("label", sort=False, observed=True)["elapsed"].agg([
        ("count", "count"),
        ("mean_ms", "mean"),
        ("std_ms", "std"),
        ("median_ms", "median"),
        ("p90_ms", lambda s: p(s, 90)),
        ("p95_ms", lambda s: p(s, 95)),
        ("p99_ms", lambda s: p(s, 99)),
        ("min_ms", "min"),
        ("max_ms", "max"),
    ])

    factors = (
        df.groupby("label", sort=False)["users_factor"]
          .first()
          .reindex(out.index)
          .replace(0, 1)
          .fillna(1)
          .astype(float)
    )
    out["count"] = out["count"].astype(float) / factors

    out = out[["count", "mean_ms", "std_ms", "median_ms",
               "p90_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"]]
    return out


def _label_order(df: pd.DataFrame) -> List[str]:
    if pd.api.types.is_categorical_dtype(df["label"]):
        return list(df["label"].cat.categories)
    return list(pd.unique(df["label"]))


def _wrap(s: Optional[str], width: int) -> Optional[str]:
    if not s or width <= 0:
        return s
    return textwrap.fill(s, width)

def plot_box(
    df: pd.DataFrame,
    outpath: Path,
    title_suffix: str = "",
    title: Optional[str] = None,
    title_width: int = 0,
    fig_size: Optional[Tuple[float, float]] = None
):
    order = _label_order(df)
    df2 = df.copy()
    df2["label"] = pd.Categorical(df2["label"], categories=order, ordered=True)

    plt.figure(figsize=fig_size) if fig_size else plt.figure()

    ax = df2.boxplot(column="elapsed", by="label", grid=True)
    plt.suptitle("")
    final_title = (title or "Elapsed Time (ms) by dataset") + title_suffix
    ax.set_title(_wrap(final_title, title_width), wrap=True)
    ax.set_xlabel("")
    ax.set_ylabel("Elapsed Time (ms)")

    if "error" in df.columns:
        err_rate = (
            df.groupby("label", sort=False)["error"]
              .mean()
              .reindex(order)
              .fillna(0.0)
        )
        if (err_rate > 0).any():
            ymin, ymax = ax.get_ylim()
            span = ymax - ymin if ymax > ymin else 1.0
            ax.set_ylim(ymin, ymax + 0.10 * span)
            for i, rate in enumerate(err_rate.tolist(), start=1):
                if rate > 0:
                    ax.text(i, ymax + 0.02 * span, f"{rate*100:.1f}% err",
                            ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    plt.savefig(outpath, dpi=200, bbox_inches="tight")
    plt.close()


def plot_ecdf(
    df: pd.DataFrame,
    outpath: Path,
    title_suffix: str = "",
    title: Optional[str] = None,
    title_width: int = 0,
    fig_size: Optional[Tuple[float, float]] = None
):
    plt.figure(figsize=fig_size) if fig_size else plt.figure()

    order = _label_order(df)
    for lbl in order:
        sel = df["label"] == lbl
        g = df.loc[sel, "elapsed"].values
        if g.size == 0:
            continue
        x = np.sort(g)
        y = np.arange(1, len(x) + 1) / len(x)  # ECDF is 0..1
        plt.plot(x, y, label=lbl)

    plt.xlabel("Elapsed (ms)")
    plt.ylabel("Cumulative fraction")
    plt.title(_wrap((title or "ECDF of Elapsed (ms)") + title_suffix, title_width), wrap=True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=200, bbox_inches="tight")
    plt.close()


def plot_histogram(
    df: pd.DataFrame,
    outpath: Path,
    title_suffix: str = "",
    title: Optional[str] = None,
    title_width: int = 0,
    fig_size: Optional[Tuple[float, float]] = None
):
    """
    Histogram with counts normalized by users_factor (per-user).
    Uses ONE common set of bin edges so bar widths are identical.
    """
    plt.figure(figsize=fig_size) if fig_size else plt.figure()

    order = _label_order(df)

    all_vals = df["elapsed"].to_numpy()
    if all_vals.size == 0:
        return
    if np.all(all_vals == all_vals[0]):
        edges = np.linspace(all_vals[0] - 0.5, all_vals[0] + 0.5, 2)
    else:
        edges = np.histogram_bin_edges(all_vals, bins=30)

    datasets, weight_sets, labels = [], [], []
    for lbl in order:
        sel = df["label"] == lbl
        g = df.loc[sel, "elapsed"].to_numpy()
        if g.size == 0:
            continue
        uf = float(df.loc[sel, "users_factor"].iloc[0]) if sel.any() else 1.0
        w = np.full(g.shape, 1.0 / (uf if uf != 0 else 1.0))
        datasets.append(g)
        weight_sets.append(w)
        labels.append(lbl)

    plt.hist(datasets, bins=edges, weights=weight_sets, alpha=0.6, label=labels, rwidth=0.98)

    plt.xlabel("Elapsed (ms)")
    plt.ylabel("Count (per user)")
    plt.title(_wrap((title or "Histogram of Elapsed (ms) — per-user") + title_suffix, title_width), wrap=True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=200, bbox_inches="tight")
    plt.close()


def plot_summary_table(
    summary_df: pd.DataFrame,
    outpath: Path,
    title: Optional[str] = None,
    title_width: int = 0
):
    display_df = summary_df.round(3)
    nrows = len(display_df)
    fig_h = max(1.6, 0.35 * nrows + 0.6)
    fig, ax = plt.subplots(figsize=(10, fig_h))
    ax.axis("off")

    tbl = ax.table(
        cellText=display_df.values,
        rowLabels=display_df.index.tolist(),
        colLabels=display_df.columns.tolist(),
        cellLoc="center",
        loc="center"
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1.0, 1.2)

    if title:
        ax.text(0.5, 1.02, _wrap(title, title_width), ha="center", va="bottom",
                transform=ax.transAxes, wrap=True)

    fig.tight_layout()
    fig.savefig(outpath, dpi=300, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)

def trim_percentile_per_label(
    df: pd.DataFrame, low: float = 0.0, high: float = 99.0, order=None
) -> pd.DataFrame:
    def _trim(g):
        lo = np.percentile(g["elapsed"], low)
        hi = np.percentile(g["elapsed"], high)
        return g[(g["elapsed"] >= lo) & (g["elapsed"] <= hi)]

    out = df.groupby("label", group_keys=False, sort=False).apply(_trim)
    if order is not None:
        out["label"] = pd.Categorical(out["label"], categories=order, ordered=True)
    return out

def main():
    parser = argparse.ArgumentParser(
        description="Analyze elapsed times from any number of CSV files (uses ONLY the 'elapsed' column)."
    )
    parser.add_argument("--files", nargs="+", required=True,
                        help="List of CSV files (1..N) in desired order.")
    parser.add_argument("--labels", nargs="+", required=True,
                        help="List of labels (must match --files count).")
    parser.add_argument("--out", default="analysis_out",
                        help="Output directory (default: analysis_out)")

    parser.add_argument("--trim-plots", action="store_true",
                        help="Trim plot data by percentile per label (summary table remains raw).")
    parser.add_argument("--pct-low", type=float, default=0.0,
                        help="Lower percentile for plot trimming (default: 0.0).")
    parser.add_argument("--pct-high", type=float, default=99.0,
                        help="Upper percentile for plot trimming (default: 99.0).")

    parser.add_argument("--title", default=None,
                        help="Custom plot/summary title (e.g. 'Northwind API benchmark (ms)').")
    parser.add_argument("--title-width", type=int, default=0,
                        help="Wrap the title at N characters per line (0=disable).")
    parser.add_argument("--fig-size", type=float, nargs=2, metavar=("W", "H"),
                        help="Figure size in inches for plots (e.g. --fig-size 12 6).")

    args = parser.parse_args()

    if len(args.files) != len(args.labels):
        print(f"--files count ({len(args.files)}) must match --labels count ({len(args.labels)}).", file=sys.stderr)
        sys.exit(2)
    if not (0.0 <= args.pct_low < args.pct_high <= 100.0):
        print("Percentiles must satisfy 0.0 <= pct_low < pct_high <= 100.0", file=sys.stderr)
        sys.exit(2)

    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    dfs = []
    try:
        for path_str, label in zip(args.files, args.labels):
            dfs.append(load_elapsed_only(Path(path_str), label))
    except Exception as e:
        print(f"Error reading input files: {e}", file=sys.stderr)
        sys.exit(1)

    df_raw = pd.concat(dfs, ignore_index=True)

    df_raw["label"] = pd.Categorical(df_raw["label"], categories=args.labels, ordered=True)

    summary = summarize(df_raw).round(3)

    print("\n=== Summary (per label) — RAW (normalized count) ===")
    print(summary)

    summary_png = outdir / "summary_table_raw.png"
    summary_csv = outdir / "summary_table_raw.csv"
    summary_title = (f"{args.title} — Summary for Elapsed Time (ms)"
                     if args.title else "Summary Statistics for Elapsed (ms) — RAW (per-user normalized counts)")
    plot_summary_table(summary, summary_png, title=summary_title, title_width=args.title_width)
    summary.to_csv(summary_csv)

    if args.trim_plots:
        df_plot = trim_percentile_per_label(
            df_raw, low=args.pct_low, high=args.pct_high, order=args.labels
        ).reset_index(drop=True)
        suffix = f"_trim_p{args.pct_low:g}-{args.pct_high:g}"
        title_suffix = f" (trimmed to {args.pct_low}–{args.pct_high} percentile per label)"
        print(f"\nPlots will be trimmed per label to [{args.pct_low}, {args.pct_high}] percentiles.")
    else:
        df_plot = df_raw
        suffix = "_raw"
        title_suffix = ""

    boxplot_path = outdir / f"boxplot_elapsed{suffix}.png"
    ecdf_path    = outdir / f"ecdf_elapsed{suffix}.png"
    hist_path    = outdir / f"hist_elapsed{suffix}.png"

    fig_size: Optional[Tuple[float, float]] = tuple(args.fig_size) if args.fig_size else None

    plot_box(df_plot, boxplot_path, title_suffix, title=args.title,
             title_width=args.title_width, fig_size=fig_size)
    plot_ecdf(df_plot, ecdf_path, title_suffix, title=args.title,
              title_width=args.title_width, fig_size=fig_size)
    plot_histogram(df_plot, hist_path, title_suffix, title=args.title,
                   title_width=args.title_width, fig_size=fig_size)

    print(f"\nFiles written:")
    print(f"- {summary_png}")
    print(f"- {summary_csv}")
    print(f"- {boxplot_path}")
    print(f"- {ecdf_path}")
    print(f"- {hist_path}")


if __name__ == "__main__":
    main()
