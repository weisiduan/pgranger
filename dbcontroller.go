package pgranger

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const (
	dbCtrlerConfigNil              = "db controller config is nil"
	noDBCtrlerForURI               = "no db controller for %s"
	duplicateReplicaDBCtrlerForURI = "duplicate replica db controller for %s"

	dbURIParams = "dbname=%s host=%s port=%d"
)

type DBController interface {
	DB() *sqlx.DB
	URI() string
}

// DBControllerImpl implements DBController.
type DBControllerImpl struct {
	uri string
	db  *sqlx.DB
}

var _ DBController = (*DBControllerImpl)(nil)

// Utility function to find a db controller by its URI.
func findDBController(dbURIToDBCtrler map[string]DBController, dbConfig *DBConfig) (DBController, error) {
	if err := validateDBConfig(dbConfig); err != nil {
		return nil, errors.WithMessage(err, "find db controller: ")
	}
	dbURI := GetDbURI(*dbConfig.DBName, *dbConfig.Host, *dbConfig.Port)
	if dbCtrler, ok := dbURIToDBCtrler[dbURI]; ok {
		return dbCtrler, nil
	}
	return nil, fmt.Errorf(noDBCtrlerForURI, dbURI)
}

// Utility function to find a list of db controllers by their URIs.
func findDBControllers(dbURIToDBCtrler map[string]DBController, dbConfigs []*DBConfig) ([]DBController, error) {
	dbCtrlers := make([]DBController, len(dbConfigs))
	dbURIsSeen := make(map[string]bool)
	for idx, dbConfig := range dbConfigs {
		dbCtrler, err := findDBController(dbURIToDBCtrler, dbConfig)
		if err != nil {
			return nil, errors.WithMessage(err, "find db controller: ")
		}
		if _, ok := dbURIsSeen[dbCtrler.URI()]; ok {
			return nil, fmt.Errorf(duplicateReplicaDBCtrlerForURI, dbCtrler.URI())
		}
		dbCtrlers[idx] = dbCtrler
		dbURIsSeen[dbCtrler.URI()] = true
	}
	return dbCtrlers, nil
}

func buildDBControllers(
	shardingConfig *ShardingConfig,
	dbSecret *DBSecret,
	dbFactory SqlxDBFactory,
) (map[string]DBController, error) {
	if shardingConfig == nil {
		return nil, errors.New("shardingConfig is nil")
	}
	dbURIToDBCtrler := make(map[string]DBController)
	for _, dbCtrlerConfig := range shardingConfig.DBControllers {
		dbURI, dbCtrler, err := createDBControllerImpl(dbCtrlerConfig, dbSecret, dbFactory)
		if err != nil {
			return nil, errors.Wrap(err, "failed to build db controller")
		}
		if _, ok := dbURIToDBCtrler[*dbURI]; ok {
			return nil, fmt.Errorf("duplicate db controller found for %s", *dbURI)
		}
		dbURIToDBCtrler[*dbURI] = dbCtrler
	}
	return dbURIToDBCtrler, nil
}

// GetDbURI returns the identifier for a DB instance.
func GetDbURI(dbName string, dbHost string, dbPort int) string {
	return fmt.Sprintf(dbURIParams, dbName, dbHost, dbPort)
}

func createDBControllerImpl(
	dbCtrlerConfig *DBControllerConfig,
	dbSecret *DBSecret,
	dbFactory SqlxDBFactory,
) (*string, *DBControllerImpl, error) {
	if dbCtrlerConfig == nil {
		return nil, nil, errors.New(dbCtrlerConfigNil)
	}
	if err := validateDBConfig(dbCtrlerConfig.DB); err != nil {
		return nil, nil, errors.WithMessage(err, "in db controller config: ")
	}
	dbURI, db, err := dbFactory.CreateSqlxDB(dbCtrlerConfig.DB, dbSecret)
	if err != nil {
		return nil, nil, err
	}
	return dbURI, &DBControllerImpl{uri: *dbURI, db: db}, nil
}

// DB returns the *sqlx.DB client.
func (d *DBControllerImpl) DB() *sqlx.DB {
	return d.db
}

// URI returns the URI of the db client.
func (d *DBControllerImpl) URI() string {
	return d.uri
}
