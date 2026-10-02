# tap-imis

`tap-imis` is a Singer tap for [iMIS](https://www.imis.com/), association and membership management software.

Built with the [Hotglue Singer SDK](https://github.com/hotgluexyz/HotglueSingerSDK) for Singer Taps.

## Installation

```bash
pip install tap-imis
```

Or install from git:

```bash
pip install git+https://github.com/hotgluexyz/tap-imis.git
```

## Configuration

| Setting     | Required | Description |
|-------------|----------|-------------|
| `site_url`  | Yes      | iMIS site base URL (for example `https://yourorg.imiscloud.com`) |
| `username`  | Yes      | API user for password-grant token |
| `password`  | Yes      | API password |
| `start_date`| No       | Lower bound for incremental streams (`UpdatedOn`, `EFFECTIVE_DATE`) |

Example `config.json`:

```json
{
  "site_url": "https://yourorg.imiscloud.com",
  "username": "api_user",
  "password": "your_password",
  "start_date": "2020-01-01T00:00:00Z"
}
```

Run `tap-imis --about` for the full config schema.

### Authentication

The tap uses the iMIS password grant: `POST {site_url}/Token` with `grant_type=password`. Tokens refresh in memory before expiry.

## Supported streams

| Stream         | Replication key   | Primary key(s)              | Replication |
|----------------|-------------------|-----------------------------|-------------|
| `contacts`     | `UpdatedOn`       | `PartyId`                   | INCREMENTAL (`UpdatedOn=ge:{bookmark}`) |
| `activities`   | `EFFECTIVE_DATE`  | `PartyId`, `ActivityId`     | INCREMENTAL (`EFFECTIVE_DATE=ge:{bookmark}`) |
| `event`        | —                 | `EventId`                   | FULL_TABLE |
| `group`        | —                 | `GroupId`                   | FULL_TABLE |
| `group_member` | —                 | `GroupMemberId`             | FULL_TABLE |

All records are converted from iMIS serialization to plain JSON. `$type` keys are dropped, `$values` collections become arrays, and typed values like `{"$value": true}` become plain values. Generic property bags (such as `AdditionalAttributes`) become objects, with empty strings mapped to `null`.

Schemas are discovered per tenant. Types inferred from a sample page of records win over `/metadata` types, and metadata fills in fields missing from the sample.

## Usage

```bash
tap-imis --version
tap-imis --help
tap-imis --config config.json --discover > catalog.json
tap-imis --config config.json --catalog catalog.json
```

## Developer setup

```bash
python -m venv .venv
.venv/bin/pip install -e .
.venv/bin/pip install pytest tox ruff
.venv/bin/pytest tap_imis/tests -v
tox
```
