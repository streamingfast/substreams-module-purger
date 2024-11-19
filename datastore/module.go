package datastore

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"time"
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

func ModulesToPurge(db *sqlx.DB, subfolder string) ([]*ModuleCache, error) {
	var modules []*ModuleCache

	err := db.Select(&modules, purgeModuleQuery(subfolder))
	if err != nil {
		return nil, fmt.Errorf("querying module cache: %w", err)
	}

	return modules, nil
}

func purgeModuleQuery(subfolder string) string {
	if subfolder == "" {
		return `
	with youngest_file as (
		select bucket, network, subfolder, max(created_at) as youngest_file_creation_date
		from cost_estimator.files
		where deleted_at is null
		and filetype != 1
		group by bucket, network, subfolder
		order by youngest_file_creation_date
	)

	select * from youngest_file where youngest_file_creation_date < now() - interval '1 month' limit 1;`
	}

	return fmt.Sprintf(`
	with youngest_file as (
		select bucket, network, subfolder, max(created_at) as youngest_file_creation_date
		from cost_estimator.files
		where deleted_at is null
		and filetype != 1
		and subfolder = '%s'
		group by bucket, network, subfolder
		order by youngest_file_creation_date
	)

	select * from youngest_file where youngest_file_creation_date < now() - interval '1 month' limit 1;`, subfolder)
}
