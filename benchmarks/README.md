# Benchmarks

`run_benchmark.py` compares the Spark-native checks of jpdq-oga with pandas-UDF baselines on synthetic
data.  The numbers that are quoted in the README come from the GitHub Actions workflow
[`benchmark.yml`](../.github/workflows/benchmark.yml) and are recorded, with the runner description, in
[`BENCHMARK.md`](../BENCHMARK.md).

```bash
pip install -e ".[benchmark]"          # JDK 17 required
python benchmarks/run_benchmark.py --rows 10000000 --repeats 3 --output-dir benchmark-output
```

## What is compared

| group | native | baselines |
| --- | --- | --- |
| `corporate_number` | `corporate_number_problem` expression | pandas UDF over python-stdnum (`jp.cn.is_valid`, an existing library); pandas UDF over the pure-Python reference validator; hand-vectorised numpy pandas UDF |
| `local_gov_code` | `local_gov_code_problem` | pandas UDF over the pure-Python validator |
| `wareki_date` | `wareki_problem` | pandas UDF over the pure-Python validator |
| `postal_code_exists` | DQX `is_existing_jp_postal_code` (broadcast join, 120,000-row reference) | pandas UDF with a Python `set` |
| `dqx_four_row_rules` | `apply_checks_by_metadata` with the four row-level rules | (absolute cost of the DQX path only) |

## Method

- Data is generated with Spark expressions (10M rows by default), written to Parquet once, and read by
  every pass, so all implementations pay the same scan cost.
- A pass evaluates the check on the whole column and discards the result (`noop` sink).
- Per implementation: one warm-up pass on a tenth of the rows, then `--repeats` timed passes; the
  median is reported (all passes are in `results.json`).
- **Correctness gate**: before timing, every implementation of a group must produce identical counts of
  true / false / NULL.  A disagreement aborts the run.
- Pandas UDFs use Spark's defaults (Arrow enabled, 10,000 rows per batch) and the same `local[*]`
  master and driver memory as the native runs.

## Limits of the comparison

- A single machine in local mode.  Cluster behaviour (shuffle, Photon, many executors) is not measured.
- The pure-Python and stdnum baselines are the naive "wrap it in a pandas UDF" implementations; the
  vectorised numpy baseline shows what careful pandas code achieves for the one check where that is easy.
- Synthetic data: random digits (most corporate numbers are invalid) and a fixed set of era-date strings.
