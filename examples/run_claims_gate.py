import pandas as pd

from data_gates.claims_gate import ClaimsGate, GateConfig


def main() -> None:
    # Load demo input
    df = pd.read_csv("examples/sample_claims.csv")

    # Optional: provide a small CPT allow-list to demonstrate the rule
    cfg = GateConfig(valid_cpt_codes=("99213", "93000", "80053"))

    gate = ClaimsGate(config=cfg)
    valid_df, quarantine_df = gate.validate(df)

    print("=== Gate Results ===")
    print(f"Total rows:       {len(df)}")
    print(f"Valid rows:       {len(valid_df)}")
    print(f"Quarantined rows: {len(quarantine_df)}")

    if len(quarantine_df) > 0:
        print("\n=== Quarantine Sample ===")
        cols = [c for c in ["claim_id", "member_id", "provider_npi", "service_start_local", "cpt_code",
                            "severity", "primary_reason", "quarantine_reasons"] if c in quarantine_df.columns]
        print(quarantine_df[cols].head(10).to_string(index=False))

    print("\n=== Metrics ===")
    print(gate.metrics(df))


if __name__ == "__main__":
    main()
