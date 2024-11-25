## Substreams Cache Purger

The `purger` command is used to purge old module cache data... 

### Prerequisites

1. **Authenticate with GCloud**  
   Before running the command, ensure you’re authenticated with Google Cloud by running:
   ```bash
   gcloud auth login
   ```

2. Port-forward localhost:5432 to the DB if necessary

### Run it

* Select the network by its namespace (ex: `sol-mainnet`)
* For each network, run: `runPurger --database-dsn="postgres://${PGUSER}:${PGPASS}@localhost:5432/postgres?enable_incremental_sort=off&sslmode=disable" --network=sol-mainnet`
* NOTE: the command above will ask for confirmation at every module hash to purge. Use `-f` to skip the confirmation.

