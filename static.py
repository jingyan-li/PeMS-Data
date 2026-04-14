def get_dataset_column_name(n_cols):
    # assign canonical PeMS station 5-min column names
    base_cols = [
        "Timestamp", "Station", "District", "Freeway", "Direction", "Lane_Type",
        "Station_Length", "Samples", "Percent_Observed", "Total_Flow",
        "Avg_Occupancy", "Avg_Speed"
    ]
    remaining = n_cols - len(base_cols)

    if remaining < 0 or remaining % 5 != 0:
        raise ValueError(f"Unexpected column count ({n_cols}). Cannot map lane columns cleanly.")

    n_lanes = remaining // 5
    lane_cols = []
    for lane in range(1, n_lanes + 1):
        lane_cols.extend([
            f"Lane_{lane}_Samples",
            f"Lane_{lane}_Flow",
            f"Lane_{lane}_Avg_Occupancy",
            f"Lane_{lane}_Avg_Speed",
            f"Lane_{lane}_Percent_Observed",
        ])

    header_cols = base_cols + lane_cols
    
    return header_cols