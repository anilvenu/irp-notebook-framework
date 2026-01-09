"""
Investigate why compound perils (Surge + Wind, etc.) have empty regionPerilSimulationSet.

Check simulation sets, PET metadata, and software version map for relevant entries.
"""

import json
from helpers.irp_integration import IRPClient


def main():
    print("Initializing IRP client...")
    irp_client = IRPClient()
    ref_data = irp_client.reference_data

    # Get all simulation sets and check for surge/compound entries
    print("\n" + "="*60)
    print("SIMULATION SETS - Checking for WS (Windstorm) entries")
    print("="*60)

    sim_sets = ref_data.get_all_simulation_sets()
    print(f"Total simulation sets: {len(sim_sets)}")

    # Filter for WS peril
    ws_sim_sets = [s for s in sim_sets if s.get('perilCode') == 'WS']
    print(f"WS (Windstorm) simulation sets: {len(ws_sim_sets)}")

    # Check for any that mention surge or compound
    print("\nWS simulation sets (showing name and modelRegionCode):")
    for s in ws_sim_sets[:20]:  # Show first 20
        print(f"  id={s.get('id')}, modelRegionCode={s.get('modelRegionCode')}, "
              f"eventRateSchemeId={s.get('eventRateSchemeId')}")
        print(f"    name: {s.get('name')}")

    # Check for CS (Severe Convective Storm) peril
    print("\n" + "="*60)
    print("SIMULATION SETS - Checking for CS (Convective Storm) entries")
    print("="*60)

    cs_sim_sets = [s for s in sim_sets if s.get('perilCode') == 'CS']
    print(f"CS simulation sets: {len(cs_sim_sets)}")

    for s in cs_sim_sets[:10]:
        print(f"  id={s.get('id')}, modelRegionCode={s.get('modelRegionCode')}, "
              f"eventRateSchemeId={s.get('eventRateSchemeId')}")
        print(f"    name: {s.get('name')}")

    # Check software model version map for patterns
    print("\n" + "="*60)
    print("SOFTWARE MODEL VERSION MAP - WS entries for NAWS region")
    print("="*60)

    version_maps = ref_data.get_all_software_model_version_map()
    print(f"Total version maps: {len(version_maps)}")

    # Filter for NAWS
    naws_maps = [v for v in version_maps if v.get('modelRegionCode') == 'NAWS']
    print(f"NAWS entries: {len(naws_maps)}")
    for v in naws_maps[:5]:
        print(f"  softwareVersionCode={v.get('softwareVersionCode')}, "
              f"modelVersionCode={v.get('modelVersionCode')}, "
              f"perilCode={v.get('perilCode')}")

    # Check for USCS (US Convective Storm)
    print("\n" + "="*60)
    print("SOFTWARE MODEL VERSION MAP - CS entries for USCS region")
    print("="*60)

    uscs_maps = [v for v in version_maps if v.get('modelRegionCode') == 'USCS']
    print(f"USCS entries: {len(uscs_maps)}")
    for v in uscs_maps[:5]:
        print(f"  softwareVersionCode={v.get('softwareVersionCode')}, "
              f"modelVersionCode={v.get('modelVersionCode')}, "
              f"perilCode={v.get('perilCode')}")

    # Check if there's something special about surge
    print("\n" + "="*60)
    print("Searching for 'Surge' in simulation set names")
    print("="*60)

    surge_sets = [s for s in sim_sets if 'surge' in s.get('name', '').lower()]
    print(f"Simulation sets mentioning 'surge': {len(surge_sets)}")
    for s in surge_sets[:10]:
        print(f"  id={s.get('id')}, perilCode={s.get('perilCode')}, "
              f"modelRegionCode={s.get('modelRegionCode')}")
        print(f"    name: {s.get('name')}")

    # Check PET metadata for any patterns
    print("\n" + "="*60)
    print("PET METADATA - Checking for WS entries")
    print("="*60)

    pet_metadata = ref_data.get_all_pet_metadata()
    print(f"Total PET entries: {len(pet_metadata)}")

    ws_pets = [p for p in pet_metadata if p.get('perilCode') == 'WS']
    print(f"WS PET entries: {len(ws_pets)}")
    for p in ws_pets[:5]:
        print(f"  id={p.get('id')}, modelRegionCode={p.get('modelRegionCode')}, "
              f"numberOfPeriods={p.get('numberOfPeriods')}")


if __name__ == '__main__':
    main()
