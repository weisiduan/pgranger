// Package pgranger implements a sharding library to be run in the application processes.
//
// Examples:
//	For initiation:
//	pgRanger, err := pgranger.NewPGRanger(shardingConfigStr, &dbSecret, dbFactory)
//	if err != nil {
//	  log.Fataf("server cannot init PGRanger : %s", err)
//	}
//	defer pgRanger.Close()
//
// For pointwise routing to a db instance/shard:
// At top of request handler.
//	dbCtrler, err := reqHandler.pgRanger.FindDBControllerByUUID(shardingFieldName, shardingFieldVal, false)
//	if err != nil {
//		return nil, err
//	}
//	tx, err := dbCtrler.DB().Begin()
//	err = tx.Exec(...)
//	err = tx.Commit()
package pgranger

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"

	"github.com/jmoiron/sqlx"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

const (
	cannotFindShardingMapping = "cannot find shard mapping for %s"
)

type SqlxDBFactory interface {
	CreateSqlxDB(dbConfig *DBConfig, dbSecret *DBSecret) (*string, *sqlx.DB, error)
}

// shardingData contains all the sharding information in memory, the sharding information should be loaded from
// persistent storage. Future work will support data migration cross db instances, and as part of it, shardingData will
// be replaced with a swap in PGRanger in live process.
type shardingData struct {
	config              *ShardingConfig
	fieldToNamespace    map[string]*Namespace
	dbURIToDBController map[string]DBController
}

// PGRanger manages all sharding information and behaviors around sharding.
type PGRanger struct {
	shardingData  *shardingData
	dbSecret      *DBSecret
	sqlxDbFactory SqlxDBFactory
}

func LoadLocalConfigFile(localConfigFilePath string) (*string, error) {
	bytes, err := ioutil.ReadFile(localConfigFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "LoadLocalConfigFile")
	}
	shardingConfigStr := string(bytes)
	return &shardingConfigStr, nil
}

// NewPGRanger creates the PGRanger.
func NewPGRanger(
	shardingConfigStr *string,
	dbSecret *DBSecret,
	dbFactory SqlxDBFactory,
) (*PGRanger, error) {
	if dbSecret == nil {
		return nil, errors.New("dbSecret is nil")
	}

	shardingData, err := CreateShardingData(shardingConfigStr, dbSecret, dbFactory)
	if err != nil {
		return nil, errors.WithMessage(err, "in NewPGRanger")
	}

	return &PGRanger{
		shardingData:  shardingData,
		dbSecret:      dbSecret,
		sqlxDbFactory: dbFactory,
	}, nil
}

// CreateShardingData creates the sharding data object.
func CreateShardingData(
	shardingConfigStr *string,
	dbSecret *DBSecret,
	dbFactory SqlxDBFactory,
) (*shardingData, error) {
	if shardingConfigStr == nil || *shardingConfigStr == "" {
		return nil, errors.New("shardingConfigStr is empty")
	}
	shardingConfig := &ShardingConfig{}
	if err := json.Unmarshal([]byte(*shardingConfigStr), shardingConfig); err != nil {
		return nil, errors.WithMessage(err, "in CreateShardingData")
	}
	if shardingConfig.Version == nil {
		return nil, errors.New("config version num unset")
	}

	dbURIToDBCtrler, err := buildDBControllers(shardingConfig, dbSecret, dbFactory)
	if err != nil {
		return nil, errors.WithMessagef(err, "in CreateShardingData")
	}
	// Build all namespaces.
	fieldNameToNamespace, err := buildNamespaces(shardingConfig, dbURIToDBCtrler)
	if err != nil {
		return nil, errors.WithMessagef(err, "in CreateShardingData")
	}
	return &shardingData{
		config:              shardingConfig,
		fieldToNamespace:    fieldNameToNamespace,
		dbURIToDBController: dbURIToDBCtrler,
	}, nil
}

// Close will close the db connections in the struct. If there are multiple errors in closing we keep the last one.
func (m *PGRanger) Close() error {
	var err error
	for _, namespace := range m.shardingData.fieldToNamespace {
		for _, shardingRange := range namespace.ranges {
			err = multierr.Append(err, shardingRange.primaryDBController.DB().Close())
			for _, replicaDBMgr := range shardingRange.replicaDBControllers {
				err = multierr.Append(err, replicaDBMgr.DB().Close())
			}
		}
	}
	return err
}

// FindDBControllerByUUID finds a db controller by uuid.
func (m *PGRanger) FindDBControllerByUUID(
	shardingFieldName string, shardingFieldVal uuid.UUID, isReplica bool,
) (DBController, error) {
	uuidBytes, err := shardingFieldVal.MarshalBinary()
	if err != nil {
		return nil, errors.Wrapf(
			err, "cannot marshal uuid for key %s: %s", shardingFieldName, shardingFieldVal.String())
	}
	dbCtrler, err := m.FindDBController(shardingFieldName, uuidBytes, isReplica)
	if err != nil {
		return nil, err
	}
	return dbCtrler, nil
}

// FindDBController finds a db controller by []byte.
func (m *PGRanger) FindDBController(
	shardingFieldName string, shardingFieldVal []byte, isReplica bool,
) (DBController, error) {
	namespace, ok := m.shardingData.fieldToNamespace[shardingFieldName]
	if !ok {
		return nil, errors.Errorf(cannotFindShardingMapping, shardingFieldName)
	}
	for _, shardingRange := range namespace.ranges {
		if shardingRange.contains(shardingFieldVal) {
			return getDBController(shardingRange, isReplica)
		}
	}
	return nil, errors.Errorf(
		"cannot find db controller for %s %s: config might be wrong", shardingFieldName,
		hex.EncodeToString(shardingFieldVal))
}

func getDBController(shardingRange *Range, isReplica bool) (DBController, error) {
	if !isReplica {
		return shardingRange.primaryDBController, nil
	}
	if len(shardingRange.replicaDBControllers) <= 0 {
		return nil, errors.New("No replica db found")
	}
	return shardingRange.replicaDBControllers[rand.Intn(len(shardingRange.replicaDBControllers))], nil
}

// FindDBControllers finds the db controllers covering the sharding field values.
func (m *PGRanger) FindDBControllers(
	shardingFieldName string, shardingFieldValues [][]byte, isReplica bool,
) ([]string, map[string][][]byte, map[string]DBController, error) {
	namespace, ok := m.shardingData.fieldToNamespace[shardingFieldName]
	if !ok {
		return nil, nil, nil, errors.Errorf(cannotFindShardingMapping, shardingFieldName)
	}

	dbURIToDBCtrler := make(map[string]DBController)
	dbURIs := make([]string, len(shardingFieldValues))
	dbURIToShardingFieldVals := make(map[string][][]byte)
	for idx, shardingFieldValue := range shardingFieldValues {
		for _, shardingRange := range namespace.ranges {
			if shardingRange.contains(shardingFieldValue) {
				dbCtrler, err := getDBController(shardingRange, isReplica)
				if err != nil {
					return nil, nil, nil, errors.WithMessage(err, "in FindDBControllers")
				}
				dbURI := dbCtrler.URI()

				dbURIToDBCtrler[dbURI] = dbCtrler
				dbURIs[idx] = dbURI

				if _, ok := dbURIToShardingFieldVals[dbURI]; !ok {
					dbURIToShardingFieldVals[dbURI] = make([][]byte, 0)
				}
				dbURIToShardingFieldVals[dbURI] = append(dbURIToShardingFieldVals[dbURI], shardingFieldValues[idx])

				break
			}
		}
		// If not found matching range for a shardingFieldValue, return error.
		if dbURIs[idx] == "" {
			return nil, nil, nil, errors.Errorf(
				"cannot find db controller for %s %s: config might be wrong", shardingFieldName,
				hex.EncodeToString(shardingFieldValue))
		}
	}
	return dbURIs, dbURIToShardingFieldVals, dbURIToDBCtrler, nil
}

// FindAllDBControllers returns all db controllers.
// Make a copy so users can not accidentally modify shardingData.dbURIToDBController.
func (m *PGRanger) FindAllDBControllers(isReplica bool) (map[string]DBController, error) {
	dbURIToDBCtrler := make(map[string]DBController)
	for fieldName, namespace := range m.shardingData.fieldToNamespace {
		for _, shardingRange := range namespace.ranges {
			dbCtrler, err := getDBController(shardingRange, isReplica)
			if err != nil {
				return nil, errors.WithMessage(
					err, fmt.Sprintf("in FindAllDBControllers fieldName:%v range:%s", fieldName, shardingRange))
			}
			dbURIToDBCtrler[dbCtrler.URI()] = dbCtrler
		}
		break
	}
	return dbURIToDBCtrler, nil
}

type DBRunnable interface {
	Run(resChan chan interface{}, errChan chan error)
}

// FanOutResult is the result from the fan out execution.
type FanOutResult struct {
	// DBResults contains result per DB.
	DBResults map[string]interface{}
	// ErrorDetails contains error per DB.
	ErrorDetails map[string]error
}

// FanOutContext is the context/input for the fan out execution.
type FanOutContext struct {
	// DBURIToDBRunnable contains mapping from DB URIs to DBRunnables.
	DBURIToDBRunnable map[string]DBRunnable
}

// PointFanOutProcessor is to be implemented by users who needs fan out to db instances. It is to be provided as an
// input to ExecuteFanOut().
// There are two key steps:
// 1. The instance of implementation class of PointFanOutProcessor should provide the sharding field name and values
//    for the fan out. For this, implement ProvideShardingFieldNameAndValues(). The PGRanger will then find the correct
//    db instances hosting the sharding field values. If fan out to all db instances are needed, return nil from
//    ProvideShardingFieldNameAndValues().
// 2. Now that PGRanger has decided the sharding field values and the corresponding db instances, it needs the
//    runnables for "what to do" on each db instance. For this purpose, PGRanger will feed the sharding field values
//    and db instances into the CreateFanOutContext or CreateFanOutContextForAllDBs for db runnable creation.
// PGRanger will then run all the runnables returned from step 2 in parallel in ExecuteFanOut.
type PointFanOutProcessor interface {
	// Name of the PointFanOutProcessor instance.
	Name() string
	// ProvideShardingFieldNameAndValues provides the sharding field name and values for PGRanger to look up.
	ProvideShardingFieldNameAndValues() (*string, [][]byte)
	// CreateFanOutContextForAllDBs is a callback that will provide runnables for all db shards. Implement this if your
	// use case needs requests to all db shards.
	CreateFanOutContextForAllDBs(dbURIToDBCtrler map[string]DBController) (*FanOutContext, error)
	// CreateFanOutContext is a callback to provide the runnables for specific db shards with input which are decided
	// by pgranger based on output of FetchShardingFieldNameAndValues.
	CreateFanOutContext(
		dbURIs []string,
		dbURIToShardingFieldVals map[string][][]byte,
		dbURIToDBCtrler map[string]DBController,
	) (*FanOutContext, error)
}

// ExecuteFanOut is the entry point for fanning out to a bunch of dbs from application logic.
// fanOutProcessor is the callback provided by the application to be called during the processing. The application will
// need to implement and pass in this PointFanOutProcessor interface. PointFanOutProcessor is named so because the fan
// out input are point values. In the future we could have range values as fan out input.
func (m *PGRanger) ExecuteFanOut(fanOutProcessor PointFanOutProcessor, isReplica bool) (*FanOutResult, error) {
	shardingFieldName, shardingFieldValues := fanOutProcessor.ProvideShardingFieldNameAndValues()
	fanOutToAllDBs := false
	if shardingFieldName == nil {
		fanOutToAllDBs = true
	}

	var err error
	var fanOutContext *FanOutContext
	var dbURIs []string
	var dbURIToShardingFieldVals map[string][][]byte
	var dbURIToDBCtrler map[string]DBController
	if fanOutToAllDBs {
		dbURIToDBCtrler, err = m.FindAllDBControllers(isReplica)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		fanOutContext, err = fanOutProcessor.CreateFanOutContextForAllDBs(dbURIToDBCtrler)
	} else {
		if shardingFieldValues == nil || len(shardingFieldValues) == 0 {
			return nil, errors.Errorf("shardingFieldValues empty or nil for %s", *shardingFieldName)
		}
		dbURIs, dbURIToShardingFieldVals, dbURIToDBCtrler, err = m.FindDBControllers(
			*shardingFieldName, shardingFieldValues, isReplica)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		fanOutContext, err = fanOutProcessor.CreateFanOutContext(dbURIs, dbURIToShardingFieldVals, dbURIToDBCtrler)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if fanOutContext == nil {
		return nil, errors.Errorf("no fanOutContext from PointFanOutProcessor %s", fanOutProcessor.Name())
	}
	return Execute(fanOutContext)
}

// Execute executes fanout runnables in parallel.
func Execute(fanOutContext *FanOutContext) (*FanOutResult, error) {
	resChans := make(map[string]chan interface{})
	errChans := make(map[string]chan error)
	for dbURI, runnable := range fanOutContext.DBURIToDBRunnable {
		resChans[dbURI] = make(chan interface{}, 1)
		errChans[dbURI] = make(chan error, 1)
		go runnable.Run(resChans[dbURI], errChans[dbURI])
	}
	var err error
	errorDetails := make(map[string]error)
	for dbURI, errChan := range errChans {
		errorDetails[dbURI] = <-errChan
		err = multierr.Append(err, errorDetails[dbURI])
	}
	dbResults := make(map[string]interface{})
	for dbURI, resChan := range resChans {
		dbResults[dbURI] = <-resChan
	}
	return &FanOutResult{DBResults: dbResults, ErrorDetails: errorDetails}, err
}

// ConvertUUIDsToBytes converts uuids to [][]byte
func ConvertUUIDsToBytes(shardingFieldUUIDs []uuid.UUID) ([][]byte, error) {
	shardingFieldValues := make([][]byte, len(shardingFieldUUIDs))
	for idx, shardingFieldUUID := range shardingFieldUUIDs {
		uuidBytes, err := shardingFieldUUID.MarshalBinary()
		if err != nil {
			return nil, errors.Wrapf(
				err, "cannot marshal uuid for %s", shardingFieldUUID.String())
		}
		shardingFieldValues[idx] = uuidBytes
	}
	return shardingFieldValues, nil
}
