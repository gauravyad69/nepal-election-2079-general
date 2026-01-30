# Nepal Election 2079 (General) — extracted dataset

This repo contains a structured export of Nepal’s **General Election 2079** results data (plus boundary GeoJSON suitable for mapping) downloaded from the Election Commission results site.

## What’s in here

- `extract_general_2079.py`: downloader/normalizer used to build the dataset.
- `out/elections/2079/general/normalized/`: normalized JSON datasets and indexes.
- `out/elections/2079/general/media/`: downloaded candidate photos and symbol images.
- `out/elections/2079/general/geo/`: boundary GeoJSON files.

Key entrypoints:

- `out/elections/2079/general/normalized/index.json`
- `out/elections/2079/general/geo/index.json`

## Join keys (results ↔ geometry)

The GeoJSON files include join fields in `properties`:

- Province: `STATE_C`
- District: `DCODE`
- HoR constituency: `F_CONST`
- PA constituency: `P_CONST`

The normalized index embeds pointers to the relevant geo files for each constituency.

## Reproduce

From the repo root:

- `python3 extract_general_2079.py --out out --download-geo`

## Data source

Downloaded from `https://result.election.gov.np/`.
