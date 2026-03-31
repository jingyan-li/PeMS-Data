# %%
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
# %%
link_station_mapping_file = os.path.join(r"To_Sean_DTA\Validation data\PeMS_2019_data\PeMS_correspondence_030917.csv")
file = os.path.join(r"To_Sean_DTA\Validation data\PeMS_2019_data\SCAG_hour_Avg_TWeekday2_T1_flitered.csv")

# %%
link_station_mapping = pd.read_csv(link_station_mapping_file)
link_station_mapping = link_station_mapping[['VDS', 'Dir', 'LinkID']]
link_station_mapping.rename(columns={'LinkID': 'LinkID_2012'}, inplace=True)
# linkID is 2012_ID
# all links are already directed
link_station_mapping['VDS'] = link_station_mapping['VDS'].astype(int)
# %%
obs_df = pd.read_csv(file)
obs_df = pd.merge(obs_df, link_station_mapping, left_on='Station', right_on='VDS', how='inner')
print(obs_df['VDS'].isna().sum())
print(obs_df['Station'].isna().sum())
# %%
obs_df = obs_df[['LinkID_2012', 'Hour', 'Flow', 'Speed']]
obs_df = obs_df.groupby(['LinkID_2012', 'Hour']).mean().reset_index()
# %%
if os.path.exists(os.path.join('./processed_data/network', 'link_2012ID_mapping.csv')):
    network_shp = pd.read_csv(os.path.join('./processed_data/network', 'link_2012ID_mapping.csv'))
else:
    network_shp_with_capacity_file = os.path.join("./To_Sean_DTA/DTA_project_network_shp/working network shp/scag_network_working.shp")
    network_shp_with_node_file = os.path.join("./To_Sean_DTA/Model_Data/Network/24r19by_links.shp")
    network_shp = gpd.read_file(network_shp_with_capacity_file, engine='pyogrio', use_arrow=False)
    network_shp1 = gpd.read_file(network_shp_with_node_file, engine='pyogrio', use_arrow=False)
    network_shp = pd.merge(network_shp, network_shp1.loc[:, ['ID', 'FROM_ID', 'TO_ID']], on='ID')
    network_shp = network_shp[network_shp['FROM_ID'] != network_shp['TO_ID']]
    cond = (~network_shp['AB_FACILIT'].isin([200, 300])) & \
            (network_shp['MODE'] == 2)
    network_shp = network_shp.loc[cond, :]

    network_shp = network_shp[['ID', 'F2012_ID']]
    network_shp = network_shp.loc[~network_shp['F2012_ID'].isna(), :]
    network_shp['F2012_ID'] = network_shp['F2012_ID'].astype(int)
    network_shp['ID'] = network_shp['ID'].astype(int)
    network_shp.rename(columns={'F2012_ID': 'LinkID_2012'}, inplace=True)
    network_shp.to_csv(os.path.join('./processed_data/network', 'link_2012ID_mapping.csv'), index=False)
# %%
obs_df = pd.merge(obs_df, network_shp, on='LinkID_2012', how='inner')
print(obs_df['ID'].isna().sum())
# %%
obs_df = obs_df[['ID', 'LinkID_2012', 'Hour', 'Flow', 'Speed']]
obs_df.to_csv(os.path.join('./processed_data/calibration/validation_data', 'PeMS_hourly_obs_with_linkID.csv'), index=False)
# %%
from scipy.interpolate import CubicSpline
obs_df = pd.read_csv(os.path.join('./processed_data/calibration/validation_data', 'PeMS_hourly_obs_with_linkID.csv'))
# %%
# Check hourly coverage per link
hour_counts = obs_df.groupby('ID')['Hour'].count()
print(f"Total links: {hour_counts.shape[0]}")
print(f"Links with all 24 hours: {(hour_counts == 24).sum()}")
print(f"\nDistribution of hours-per-link:")
print(hour_counts.value_counts().sort_index().to_string())

incomplete = hour_counts[hour_counts < 24].reset_index()
incomplete.columns = ['ID', 'n_hours']
if len(incomplete) > 0:
    print(f"\nLinks missing at least one hour ({len(incomplete)} links):")
    print(incomplete.to_string(index=False))
    # Show which specific hours are missing for each incomplete link
    all_hours = set(range(24))
    missing_detail = []
    for link_id in incomplete['ID']:
        observed = set(obs_df.loc[obs_df['ID'] == link_id, 'Hour'])
        missing_hours = sorted(all_hours - observed)
        missing_detail.append({'ID': link_id, 'missing_hours': missing_hours})
    missing_detail_df = pd.DataFrame(missing_detail)
    print(missing_detail_df.to_string(index=False))
# %%
# Disaggregate hourly to 15-min using cubic spline interpolation.
# Knots are placed at hour midpoints (0.5, 1.5, ..., 23.5).
# Flow is rescaled per hour block to preserve hourly totals.
# Speed is interpolated directly (no conservation constraint).

t_hour_mid  = np.arange(0.5, 24, 1.0)   # 24 knot positions
t_15min_mid = np.arange(0.125, 24, 0.25) # 96 evaluation points (midpoints of each 15-min interval)

records = []
for (link_id, link_id_2012), grp in obs_df.groupby(['ID', 'LinkID_2012']):
    grp = grp.sort_values('Hour').reset_index(drop=True)
    hours  = grp['Hour'].values
    flows  = grp['Flow'].values
    speeds = grp['Speed'].values

    if len(hours) < 2:
        continue

    knot_t = t_hour_mid[hours]
    cs_flow  = CubicSpline(knot_t, flows,  bc_type='natural')
    cs_speed = CubicSpline(knot_t, speeds, bc_type='natural')

    flow_raw  = np.maximum(cs_flow(t_15min_mid),  0.0)
    speed_int = cs_speed(t_15min_mid)

    # Rescale each 4-interval block so the sum matches the original hourly flow
    flow_scaled = flow_raw.copy()
    for i, h in enumerate(hours):
        sl = slice(h * 4, h * 4 + 4)
        block_sum = flow_raw[sl].sum()
        flow_scaled[sl] = flow_raw[sl] * (flows[i] / block_sum) if block_sum > 0 else flows[i] / 4.0

    for idx in range(len(t_15min_mid)):
        h_of_day = idx // 4
        interval = idx % 4  # 0=:00, 1=:15, 2=:30, 3=:45
        # only emit rows for hours that have observed data
        if h_of_day not in hours:
            continue
        records.append({
            'ID':          link_id,
            'LinkID_2012': link_id_2012,
            'Hour':        h_of_day,
            'Interval':    interval,
            'Time_min':    h_of_day * 60 + interval * 15,
            'Flow_15min':  flow_scaled[idx],
            'Speed_15min': speed_int[idx],
        })

obs_15min_df = pd.DataFrame(records)
# %%
obs_15min_df.to_csv(
    os.path.join('./processed_data/calibration/validation_data', 'PeMS_15min_obs_with_linkID.csv'),
    index=False
)
# %%
