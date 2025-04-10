import dagster as dg
import pandas as pd

@dg.asset_check(
    asset="documents_metadata",
    blocking=True
)
def missing_document_source(documents_metadata: pd.DataFrame) -> dg.AssetCheckResult:

    params = {
        "passed": bool(documents_metadata["source"].isna().sum() == 0)
    }

    if not params["passed"]:
        params["description"] = "There are missing document sources. Please further investigate."
    
    return dg.AssetCheckResult(**params)