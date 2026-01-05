"""
Check subPerilId and other fields that might differ between compound and simple perils.
"""

from helpers.irp_integration import IRPClient


def main():
    print("Initializing IRP client...")
    irp_client = IRPClient()
    analysis_mgr = irp_client.analysis

    # Test cases
    test_cases = [
        (5042, "CBHU - Wind Only", "populated"),
        (5041, "CBHU_NT - Wind Only", "populated"),
        (4940, "USHU - Surge + Wind", "empty"),
        (4937, "USHU - Surge + Wind", "empty"),
        (4985, "USOW - Tornado + Hail + Wind", "empty"),
        (4951, "USOW - Tornado + Hail + Wind", "empty"),
    ]

    print(f"\n{'App ID':<10} {'Scenario':<35} {'subPeril':<25} {'subPerilId':<12} {'Expected'}")
    print("-" * 100)

    for app_id, scenario, expected in test_cases:
        results = analysis_mgr.search_analyses(filter=f"appAnalysisId={app_id}")
        if not results:
            print(f"{app_id:<10} NOT FOUND")
            continue

        analysis = results[0]
        sub_peril = analysis.get('subPeril', '')
        sub_peril_id = analysis.get('subPerilId', '')

        print(f"{app_id:<10} {scenario:<35} {sub_peril:<25} {sub_peril_id:<12} {expected}")

    # Also check the subPerilId values
    print("\n\nUnique subPerilId values seen:")
    all_sub_peril_ids = {}
    for app_id, scenario, expected in test_cases:
        results = analysis_mgr.search_analyses(filter=f"appAnalysisId={app_id}")
        if results:
            analysis = results[0]
            sub_peril = analysis.get('subPeril', '')
            sub_peril_id = analysis.get('subPerilId', 0)
            all_sub_peril_ids[sub_peril_id] = sub_peril

    for spid, sp in sorted(all_sub_peril_ids.items()):
        print(f"  subPerilId={spid}: {sp}")


if __name__ == '__main__':
    main()
