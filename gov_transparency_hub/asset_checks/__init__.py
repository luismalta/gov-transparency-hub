from dagster import AssetCheckResult, asset_check, AssetCheckExecutionContext, AssetKey
from gov_transparency_hub.resources import PostgresResource


def create_asset_check(asset_name: str, comparison_asset_name: str, fields_to_check: list):
    @asset_check(asset=asset_name)
    def asset_check_function(context: AssetCheckExecutionContext, postgres_resource: PostgresResource) -> AssetCheckResult:
        # Get the latest two materialization events for the current asset and comparison asset
        current_event = context.instance.get_latest_materialization_event(AssetKey([asset_name]))
        previous_event = context.instance.get_latest_materialization_event(AssetKey([comparison_asset_name]))

        current_metadata = current_event.asset_materialization.metadata if current_event else None
        previous_metadata = previous_event.asset_materialization.metadata if previous_event else None

        if not current_metadata or not previous_metadata:
            return AssetCheckResult(
                passed=False,
                metadata={"error": "Unable to retrieve metadata for comparison"}
            )

        # Extract and compare relevant metadata values
        comparison_results = {}
        passed = True

        for field in fields_to_check:
            current_value = current_metadata.get(field, None)
            previous_value = previous_metadata.get(field, None)

            if current_value is None or previous_value is None:
                comparison_results[f"{field} Error"] = "Missing metadata value"
                passed = False
            else:
                comparison_results[f"Previous {field}"] = previous_value
                comparison_results[f"Current {field}"] = current_value
                if current_value != previous_value:
                    comparison_results[f"{field} Difference"] = True
                    passed = False
                else:
                    comparison_results[f"{field} Difference"] = False

        return AssetCheckResult(
            passed=passed,
            metadata=comparison_results
        )

    return asset_check_function


check_raw_revenue_data = create_asset_check(
    asset_name="load_raw_revenue_details",
    comparison_asset_name="extract_revenue_details_df",
    fields_to_check=["row_count", "total_accumulated_month"]
)

check_raw_expense_details_data = create_asset_check(
    asset_name="load_raw_expense_details",
    comparison_asset_name="extract_expense_details_df",
    fields_to_check=["row_count", "total_value"]
)

check_raw_expense_itens_data = create_asset_check(
    asset_name="load_raw_expense_itens",
    comparison_asset_name="extract_expense_itens_df",
    fields_to_check=["row_count"]
)

check_raw_expense_invoice_data = create_asset_check(
    asset_name="load_raw_expense_invoices",
    comparison_asset_name="extract_expense_invoices_df",
    fields_to_check=["row_count"]
)

all_asset_checks = [
    check_raw_revenue_data,
    check_raw_expense_details_data,
    check_raw_expense_itens_data,
    check_raw_expense_invoice_data
]
