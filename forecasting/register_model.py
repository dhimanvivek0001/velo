"""Phase 9.1: register the day-ahead forecast in the MLflow Model Registry.

- Champion / challenger aliases: production loads models:/velo-demand-forecast@champion
- Promotion gate: a challenger replaces the champion only if its TEST WAPE is lower by more
  than PROMOTION_MARGIN_PTS (the noise level seen in the Phase 6 ablation).
- Lineage tags on every version: feature list, training table, git commit, metrics.
- Idempotent: re-running reuses existing versions instead of registering duplicates.
- Reproducibility check: reload @champion from the registry and recompute test WAPE.
"""

from __future__ import annotations

import subprocess

import mlflow
import mlflow.lightgbm
import numpy as np
from common import PROJECT_ROOT, metrics, split
from mlflow import MlflowClient
from train_fs import TABLE, feature_sets, load_training_set, prepare

MODEL_NAME = "velo-demand-forecast"
EXPERIMENT = "velo-day-ahead-demand"
CHAMPION_RUN = "fs_v0_legacy"  # Phase 7 features, served from the feature store
CHALLENGER_RUN = "fs_v0_plus_all"  # Phase 6 challenger
PROMOTION_MARGIN_PTS = (
    0.2  # challenger must beat champion on test WAPE by more than this
)


def latest_run(client: MlflowClient, exp_id: str, run_name: str):
    runs = client.search_runs(
        [exp_id],
        filter_string=f"tags.mlflow.runName = '{run_name}' and attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise SystemExit(
            f"No finished run named {run_name}. Run forecasting/train_fs.py first."
        )
    return runs[0]


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except (subprocess.CalledProcessError, OSError):
        return "unknown"


def get_or_register(client: MlflowClient, run) -> str:
    """Register the run's model once; on re-runs, reuse the existing version."""
    run_id = run.info.run_id
    existing = client.search_model_versions(
        f"name='{MODEL_NAME}' and run_id='{run_id}'"
    )
    if existing:
        version = existing[0].version
        print(f"  {run.info.run_name}: already registered as version {version}")
    else:
        version = mlflow.register_model(f"runs:/{run_id}/model", MODEL_NAME).version
        print(f"  {run.info.run_name}: registered as version {version}")

    features = feature_sets()[run.info.run_name]
    tags = {
        "feature_set": run.info.run_name,
        "features": ",".join(features),
        "n_features": str(len(features)),
        "training_table": TABLE,
        "git_commit": git_commit(),
        "valid_WAPE_pct": f"{run.data.metrics['valid_WAPE_pct']:.4f}",
        "test_WAPE_pct": f"{run.data.metrics['test_WAPE_pct']:.4f}",
        "objective": run.data.params.get("objective", ""),
    }
    for key, value in tags.items():
        client.set_model_version_tag(MODEL_NAME, version, key, value)
    return version


def main() -> None:
    mlflow.set_tracking_uri(f"sqlite:///{(PROJECT_ROOT / 'mlflow.db').as_posix()}")
    client = MlflowClient()
    exp = client.get_experiment_by_name(EXPERIMENT)

    champ_run = latest_run(client, exp.experiment_id, CHAMPION_RUN)
    chall_run = latest_run(client, exp.experiment_id, CHALLENGER_RUN)

    print(f"=== Registering into '{MODEL_NAME}' ===")
    champ_v = get_or_register(client, champ_run)
    chall_v = get_or_register(client, chall_run)
    client.update_registered_model(
        MODEL_NAME,
        description=(
            "Day-ahead (24h) trips forecast per NYC pickup zone and hour. LightGBM, Poisson loss. "
            "Features are read from the velo.features feature store. "
            "Production loads the @champion alias."
        ),
    )

    # Promotion gate
    champ_wape = champ_run.data.metrics["test_WAPE_pct"]
    chall_wape = chall_run.data.metrics["test_WAPE_pct"]
    gain = champ_wape - chall_wape
    print("\n=== Promotion gate ===")
    print(f"Champion   v{champ_v} ({CHAMPION_RUN}): test WAPE {champ_wape:.2f}%")
    print(f"Challenger v{chall_v} ({CHALLENGER_RUN}): test WAPE {chall_wape:.2f}%")
    print(f"Improvement {gain:+.2f} pts; required > {PROMOTION_MARGIN_PTS:.2f} pts")

    if gain > PROMOTION_MARGIN_PTS:
        client.set_registered_model_alias(MODEL_NAME, "previous_champion", champ_v)
        client.set_registered_model_alias(MODEL_NAME, "champion", chall_v)
        print(
            f"PROMOTED: v{chall_v} is now @champion; v{champ_v} kept as @previous_champion"
        )
    else:
        client.set_registered_model_alias(MODEL_NAME, "champion", champ_v)
        client.set_registered_model_alias(MODEL_NAME, "challenger", chall_v)
        print(
            f"NOT PROMOTED: v{champ_v} stays @champion; v{chall_v} registered as @challenger"
        )

    # Reproducibility: load @champion FROM THE REGISTRY and recompute test WAPE
    print("\n=== Reproducibility check (load from registry) ===")
    mv = client.get_model_version_by_alias(MODEL_NAME, "champion")
    features = mv.tags["features"].split(",")
    model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@champion")
    df = load_training_set()
    _train, _valid, test = split(df)
    X_te = prepare(df, features).loc[test.index]
    pred = np.clip(model.predict(X_te), 0, None)
    wape = metrics(df.loc[test.index, "trips"], pred)["WAPE_pct"]
    logged = float(mv.tags["test_WAPE_pct"])
    print(
        f"@champion = v{mv.version} ({mv.tags['feature_set']}), {len(features)} features"
    )
    print(
        f"Test WAPE recomputed from registry: {wape:.4f}%  | logged at training: {logged:.4f}%"
    )
    if abs(wape - logged) < 1e-3:
        print("PASS: the registered model reproduces its training result exactly")
    else:
        raise SystemExit("FAIL: registry model does not reproduce the logged result")

    print("\nAliases:")
    for alias, version in client.get_registered_model(MODEL_NAME).aliases.items():
        print(f"  @{alias:<18} -> v{version}")


if __name__ == "__main__":
    main()
