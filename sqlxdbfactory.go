package pgranger

import (
	"fmt"

	//_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	sslModeDisable = "disable"
	driver         = "postgres"
)

type SqlxDBFactoryImpl struct{}

func (f *SqlxDBFactoryImpl) CreateSqlxDB(dbConfig *DBConfig, dbSecret *DBSecret) (*string, *sqlx.DB, error) {
	dbURI := GetDbURI(*dbConfig.DBName, *dbConfig.Host, *dbConfig.Port)
	sslMode := sslModeDisable
	if dbConfig.SSLMode != nil {
		sslMode = *dbConfig.SSLMode
	}
	dbConnConfig := &DBConnConfig{
		DBName:   *dbConfig.DBName,
		Username: dbSecret.Username,
		Password: dbSecret.Password,
		Host:     *dbConfig.Host,
		Port:     *dbConfig.Port,
		SSLMode:  sslMode,
	}
	db, err := sqlx.Open(driver, connParams(dbConnConfig))
	if err != nil {
		return nil, nil, errors.Wrap(err, fmt.Sprintf("cannot connnect to db: %s", dbConfig))
	}
	return &dbURI, db, nil
}

func connParams(config *DBConnConfig) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.Username, config.Password, config.DBName, config.SSLMode)
}
