
# Global Health Early-Warning Risk Index (WHO)

## Snowflake setup

This project can optionally load WHO indicator data into Snowflake during ingestion.

1. Create a local .env file at the project root with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

2. Run ingestion with Snowflake enabled:

```bash
python scripts/ingest_who.py --indicator MDG_0000000007 --snowflake
```

Optional flags:

- --snowflake-table: override the target table name (default who_<indicator>)
- --snowflake-batch-size: change insert batch size (default 1000)

If you omit --snowflake, data is only written to local JSONL files under data/raw/who/.
