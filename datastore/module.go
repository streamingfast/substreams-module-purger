package datastore

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type ModuleCache struct {
	Bucket                   string    `db:"bucket"`
	Network                  string    `db:"network"`
	Subfolder                string    `db:"subfolder"`
	YoungestFileCreationDate time.Time `db:"youngest_file_creation_date"`
}

func (m *ModuleCache) String() string {
	return fmt.Sprintf("%s (%s) %s, %s", m.Bucket, m.Network, m.Subfolder, m.YoungestFileCreationDate)
}

func ModulesToPurge(db *sqlx.DB, network string, maxAgeDays uint64) ([]*ModuleCache, error) {
	var modules []*ModuleCache

	err := db.Select(&modules, purgeModuleQuery(network, fmt.Sprintf("%d days", maxAgeDays)))
	if err != nil {
		return nil, fmt.Errorf("querying module cache: %w", err)
	}

	return modules, nil
}

func purgeModuleQuery(network, interval string) string {
	return fmt.Sprintf(`
	with youngest_file as (
		select bucket, network, subfolder, max(created_at) as youngest_file_creation_date
		from cost_estimator.files
		where deleted_at is null
		and filetype != 1
		and network = '%s'
		group by bucket, network, subfolder
		order by youngest_file_creation_date
	)

	select * from youngest_file where youngest_file_creation_date < now() - interval '%s';`, network, interval)
}
