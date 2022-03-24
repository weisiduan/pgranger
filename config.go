package pgranger

import "github.com/pkg/errors"

type DBConnConfig struct {
	DBName   string
	Host     string
	Port     int
	Username string
	Password string
	SSLMode  string
}

type DBSecret struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type DBConfig struct {
	DBName  *string `json:"dbName"`
	Host    *string `json:"host"`
	Port    *int    `json:"port"`
	SSLMode *string `json:"sslMode"`
}

type DBControllerConfig struct {
	DB *DBConfig `json:"db"`
	// TODO: maybe add circuit breaker.
}

type RangeConfig struct {
	Begin      *string     `json:"begin"`
	End        *string     `json:"end"`
	PrimaryDB  *DBConfig   `json:"primaryDB"`
	ReplicaDBs []*DBConfig `json:"replicaDBs"`
}

type NamespaceConfig struct {
	Ranges []*RangeConfig `json:"ranges"`
}

type ShardingConfig struct {
	Version       *int32                      `json:"version"`
	Namespaces    map[string]*NamespaceConfig `json:"namespaces"`
	DBControllers []*DBControllerConfig       `json:"dbControllers"`
}

func validateDBConfig(dbConfig *DBConfig) error {
	if dbConfig == nil {
		return errors.New("dbconfig is nil")
	}
	if dbConfig.DBName == nil {
		return errors.New("dbconfig name is nil")
	}
	if dbConfig.Host == nil {
		return errors.New("dbconfig host is nil")
	}
	if dbConfig.Port == nil {
		return errors.New("dbconfig port is nil")
	}
	return nil
}
