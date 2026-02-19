import pandas as pd
from datetime import datetime, timedelta, UTC
from data.loaders import engine


def should_run(job_name: str, frequency_hours: int = 24) -> bool:
    """
    Returns True if the job should run based on the last execution time.
    """

    try:
        df = pd.read_sql("SELECT * FROM pipeline_control", engine)

        if job_name in df["job_name"].values:

            last_run = pd.to_datetime(
                df[df["job_name"] == job_name]["last_run"].iloc[0]
            )

            if last_run >= datetime.now(UTC) - timedelta(hours=frequency_hours):
                print(f"{job_name} skipped (recently updated)")
                return False

        return True

    except Exception:
        # table not existing yet
        return True


def mark_run(job_name: str):

    now = datetime.now(UTC).isoformat()

    try:
        df = pd.read_sql("SELECT * FROM pipeline_control", engine)

        if job_name in df["job_name"].values:
            df.loc[df["job_name"] == job_name, "last_run"] = now
        else:
            df = pd.concat(
                [df, pd.DataFrame([{"job_name": job_name, "last_run": now}])]
            )

    except Exception:
        df = pd.DataFrame([{"job_name": job_name, "last_run": now}])

    df.to_sql("pipeline_control", engine, if_exists="replace", index=False)

