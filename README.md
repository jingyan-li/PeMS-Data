# Data Processing for PeMS Data Clearinghouse

Download data from [PeMS Data Clearinghouse](https://pems.dot.ca.gov/?dnode=Clearinghouse).

The clearinghouse is an authenticated, JavaScript-driven website, so this folder now includes a browser-automation agent for historical downloads. The design is batch-oriented and resumable rather than real-time.

## What Was Added

| Component | Description |
|---|---|
| `historical_agent/` | Python package for planning, authenticating, and running historical download batches. |
| `config/site_profile.json` | Selector profile for the PeMS site. This is intentionally editable so we can adapt if the clearinghouse UI changes. |
| `job.example.json` | Example historical download job. |
| `run_historical_download.py` | Simple entrypoint script. |
| `requirements-pems-agent.txt` | Optional dependency file for the downloader. |

## Architecture

| Module | Purpose |
|---|---|
| `config.py` | Loads the job definition and the site profile. |
| `planner.py` | Expands a high-level historical request into date chunks, which keeps large backfills manageable. |
| `browser.py` | Runs the Playwright-based browser agent. It supports one-time auth bootstrapping and then reuses saved session state for batch downloads. |
| `storage.py` | Maintains a small ledger so completed chunks are skipped and failed ones can be retried. |
| `cli.py` | Exposes `init`, `auth`, `plan`, and `run` commands. |

## Setup

Install Playwright directly:

```bash
python -m pip install playwright
playwright install chromium
```

## Usage

### 1. Review or copy the example config

The included example is:

[`job.example.json`]

The included site selector profile is:

[`config/site_profile.json`]

If the live PeMS form uses different field names or different selectors for your target dataset type, update `site_profile.json`.

### 2. Bootstrap authentication once

This opens Chromium, lets you sign in, and stores session state locally for later runs:

```bash
cd "PeMS Data"
python run_historical_download.py auth --job job.example.json
```

If the login form matches the selectors in `site_profile.json`, you can optionally provide credentials through environment variables:

```bash
export PEMS_USERNAME="your_username"
export PEMS_PASSWORD="your_password"
python run_historical_download.py auth --job job.example.json
```

### 3. Preview the chunk plan

```bash
python run_historical_download.py plan --job job.example.json
```

This prints the expanded list of historical download chunks and target output paths.

### 4. Run the historical downloader

```bash
python run_historical_download.py run --job job.example.json 
python run_historical_download.py run --job job.metadata.json
```

For a smaller first test:

```bash
python run_historical_download.py run --job job.example.json --limit 1
```

## Job File Format

| Field | Description |
|---|---|
| `name` | Human-readable identifier used in chunk IDs and filenames. |
| `district` | PeMS district number such as `7`. |
| `data_type` | Logical dataset type to send to the form, such as `meta` or the clearinghouse value for the dataset you want. |
| `start_date`, `end_date` | Historical range in `YYYY-MM-DD`. |
| `chunk_days` | How many days to request per site submission. For large backfills, smaller chunks are safer. |
| `download_strategy` | Use `direct` for form submissions that trigger an immediate file download. Use `listing` for datasets like station 5-minute data where PeMS first shows a year/month file listing and then exposes one downloadable file per day. `Station Metadata` is handled as a listing workflow automatically. |
| `form_values` | Optional overrides for other form fields. Values may contain placeholders: `{district}`, `{data_type}`, `{start_date}`, `{end_date}`, `{start_mmddyyyy}`, `{end_mmddyyyy}` |

## Notes About Selectors

The current implementation is deliberately profile-driven because PeMS is a website workflow instead of a stable download API. The included selectors are a reasonable starting point, but you may need to adjust them after checking the live clearinghouse page for your specific historical dataset.

For station-based historical text datasets, the agent now supports the PeMS year/month file listing flow. In that mode, `Type` is selected first, `District` is refreshed from the live page, and daily files are downloaded from the listing by filename.

For `Station Metadata`, the downloader now inspects the metadata file table for the chosen district, sorts the currently listed metadata files by size, and downloads the largest one.

If a given dataset requires an extra radio button, checkbox, or a post-submit download link, add the relevant selector to `site_profile.json` and pass the needed values through `form_values`.

## Output Layout

| Item | Description |
|---|---|
| `downloads/...` | Downloaded historical files. |
| `downloads/_state/download_ledger.json` | Chunk status ledger for resume and retry behavior. |

## Datasets

| District | Code | Region |
|---|---|---|
| 3 | `d03` | Sacramento area |
| 4 | `d04` | San Francisco Bay Area |
| 5 | `d05` | Central Coast |
| 7 | `d07` | Los Angeles/Ventura |
| 8 | `d08` | San Bernardino/Riverside |
| 10 | `d10` | Stockton/Central Valley |
| 11 | `d11` | San Diego |

## Field Specification

### PeMS Stations 5 minutes

| Name | Comment | Units |
|---|---|---|
| Timestamp | The date and time of the beginning of the summary interval. For example, a time of 08:00:00 indicates that the aggregate(s) contain measurements collected between 08:00:00 and 08:04:59. Note that second values are always 0 for five-minute aggregations. The format is MM/DD/YYYY HH24:MI:SS. | |
| Station | Unique station identifier. Use this value to cross-reference with Metadata files. | |
| District | District # | |
| Freeway # | Freeway # | |
| Direction of Travel | N \| S \| E \| W | |
| Lane Type | CD (Coll/Dist), CH (Conventional Highway), FF (Fwy-Fwy connector), FR (Off Ramp), HV (HOV), ML (Mainline), OR (On Ramp) | |
| Station Length | Segment length covered by the station in miles/km. | |
| Samples | Total number of samples received for all lanes. | |
| % Observed | Percentage of individual lane points at this location that were observed (not imputed). | % |
| Total Flow | Sum of flows over the 5-minute period across all lanes. | Veh/5-min |
| Avg Occupancy | Average occupancy across all lanes over the 5-minute period. | % |
| Avg Speed | Flow-weighted average speed over the 5-minute period across all lanes. | Mph |
| Lane N Samples | Number of good samples received for lane N. | |
| Lane N Flow | Total flow for lane N over the 5-minute period normalized by the number of good samples. | Veh/5-min |
| Lane N Avg Occ | Average occupancy for lane N. | % |
| Lane N Avg Speed | Flow-weighted average of lane N speeds. | Mph |
| Lane N Observed | 1 indicates observed data, 0 indicates imputed. | |

### PeMS Station Meta data

| Name | Comment | Units |
|---|---|---|
| ID | An integer value that uniquely identifies the Station Metadata. Use this value to 'join' other clearinghouse files that contain Station Metadata | |
| Freeway | Freeway Number | |
| Freeway Direction | A string indicating the freeway direction. | |
| County Identifier | The unique number that identifies the county that contains this census station within PeMS. | |
| City | City | |
| State Postmile | State Postmile. | |
| Absolute Postmile | Absolute Postmile. | |
| Latitude | Latitude. | |
| Longitude | Longitude. | |
| Length | Length | |
| Type | Type. | |
| Lanes | Total number of lanes. | |
| Name | Name. | |
| User IDs[1-4] | User-entered string identifier. | |
