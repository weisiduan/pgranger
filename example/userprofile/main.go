// In this example, we have a user profile application which has its sharding key user_id spreaded over 4 ranges on 2
// host shards. We then perform point data access and fan out on the shards.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/weisiduan/pgranger"
)

const (
	shardingKeyUserID = "user_id"
)

func main() {
	configPath := flag.String("configpath", "", "file path for pgranger configuration file")
	flag.Parse()
	shardingConfigStr, err := pgranger.LoadLocalConfigFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	pgRanger, err := pgranger.NewPGRanger(
		shardingConfigStr,
		&pgranger.DBSecret{
			Username: "pgranger_test",
			Password: "password",
		},
		&pgranger.SqlxDBFactoryImpl{},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Example 1: point date insertion.
	userIDs := make([]uuid.UUID, 4)
	eventIDs := make([]uuid.UUID, 4)
	for i := 0; i < 4; i++ {
		userID, eventID, err := InsertUserProfileAndEvent(pgRanger, fmt.Sprintf("name%d", i))
		if err != nil {
			log.Fatalf("insert user %d %v", i, err)
		}
		userIDs = append(userIDs, *userID)
		eventIDs = append(eventIDs, *eventID)
	}

	// Example 2: fan out based on sharding key.
	userIDBytes, _ := pgranger.ConvertUUIDsToBytes(userIDs)
	_, err = pgRanger.ExecuteFanOut(&UserIDFanOutProcessor{
		userUUIDsInBytes: userIDBytes,
		name:             "UserIDFanOutProcessor",
	}, false)
	if err != nil {
		log.Fatal(err)
	}

	// Example 3: fan out based on non-sharding key.
	eventIDBytes, _ := pgranger.ConvertUUIDsToBytes(eventIDs)
	_, err = pgRanger.ExecuteFanOut(&EventIDFanOutProcessor{
		eventUUIDsInBytes: eventIDBytes,
		name:              "EventIDFanOutProcessor",
	}, false)
	if err != nil {
		log.Fatal(err)
	}

	if err := pgRanger.Close(); err != nil {
		log.Fatal(err)
	}
}

func InsertUserProfileAndEvent(pgRanger *pgranger.PGRanger, userName string) (*uuid.UUID, *uuid.UUID, error) {
	userUUID := uuid.New()
	eventUUID := uuid.New()
	dbCtrler, err := pgRanger.FindDBControllerByUUID("user_id", userUUID, false)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "InsertUserProfileAndEvent %s", userUUID)
	}
	tx, err := dbCtrler.DB().BeginTxx(context.Background(), nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "InsertUserProfileAndEvent %s", userUUID)
	}
	defer func() { tx.Rollback() }()
	_, err = tx.Exec(`INSERT INTO user_profile (user_id, name) VALUES ($1, $2)`, userUUID, userName)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "InsertUserProfileAndEvent %s", userUUID)
	}
	_, err = tx.Exec(`INSERT INTO user_event (id, user_id) VALUES ($1, $2)`, eventUUID, userUUID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "InsertUserProfileAndEvent %s", userUUID)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, errors.Wrapf(err, "InsertUserProfileAndEvent %s", userUUID)
	}
	return &userUUID, &eventUUID, nil
}

type UserIDFanOutProcessor struct {
	userUUIDsInBytes [][]byte
	name             string
}

func (p *UserIDFanOutProcessor) Name() string {
	return p.name
}

func (p *UserIDFanOutProcessor) ProvideShardingFieldNameAndValues() (*string, [][]byte) {
	userID := shardingKeyUserID
	return &userID, p.userUUIDsInBytes
}

func (p *UserIDFanOutProcessor) CreateFanOutContextForAllDBs(
	dbURIToDBCtrler map[string]pgranger.DBController,
) (*pgranger.FanOutContext, error) {
	// No need to implement when query is on sharding key.
	panic("no need to fan out to all when input is sharding key.")
}

func (p *UserIDFanOutProcessor) CreateFanOutContext(
	dbURIs []string,
	dbURIToShardingFieldVals map[string][][]byte,
	dbURIToDBCtrler map[string]pgranger.DBController,
) (*pgranger.FanOutContext, error) {
	dbURIToDBRunnable := make(map[string]pgranger.DBRunnable)
	for dbURI, dbCtrler := range dbURIToDBCtrler {
		dbURIToDBRunnable[dbURI] = &UserIDRunnableImpl{
			dbCtrler:         dbCtrler,
			userUUIDsInBytes: dbURIToShardingFieldVals[dbURI],
		}
	}
	return &pgranger.FanOutContext{
		DBURIToDBRunnable: dbURIToDBRunnable,
	}, nil
}

type UserIDRunnableImpl struct {
	dbCtrler         pgranger.DBController
	userUUIDsInBytes [][]byte
}

func (r *UserIDRunnableImpl) Run(resChan chan interface{}, errChan chan error) {
	userUUIDs := make([]uuid.UUID, len(r.userUUIDsInBytes))
	for i, idBytes := range r.userUUIDsInBytes {
		id, _ := uuid.FromBytes(idBytes)
		userUUIDs[i] = id
	}
	events := []*UserEvent{}
	query, args, err := sqlx.In("select * from user_event where user_id in (?);", userUUIDs)
	if err != nil {
		resChan <- nil
		errChan <- err
		return
	}
	query = r.dbCtrler.DB().Rebind(query)
	err = r.dbCtrler.DB().Select(&events, query, args...)
	if err == sql.ErrNoRows {
		fmt.Printf("UserIDRunnableImpl: no events found on shard: %s\n", r.dbCtrler.URI())
	} else if err != nil {
		resChan <- nil
		errChan <- err
		return
	} else {
		fmt.Printf("UserIDRunnableImpl: events %v found on shard: %s\n", events, r.dbCtrler.URI())
	}
	resChan <- nil
	errChan <- nil
	return
}

type UserEvent struct {
	ID     uuid.UUID `json:"id" db:"id"`
	UserID uuid.UUID `json:"user_id" db:"user_id"`
}

func (e *UserEvent) String() string {
	bytes, _ := json.Marshal(e)
	return string(bytes)
}

type EventIDFanOutProcessor struct {
	eventUUIDsInBytes [][]byte
	name              string
}

func (p *EventIDFanOutProcessor) Name() string {
	return p.name
}

func (p *EventIDFanOutProcessor) ProvideShardingFieldNameAndValues() (*string, [][]byte) {
	// This triggers fan out to all db instances.
	return nil, nil
}

func (p *EventIDFanOutProcessor) CreateFanOutContextForAllDBs(
	dbURIToDBCtrler map[string]pgranger.DBController,
) (*pgranger.FanOutContext, error) {
	dbURIToDBRunnable := make(map[string]pgranger.DBRunnable)
	for dbURI, dbCtrler := range dbURIToDBCtrler {
		dbURIToDBRunnable[dbURI] = &EventIDRunnableImpl{
			dbCtrler:          dbCtrler,
			eventUUIDsInBytes: p.eventUUIDsInBytes,
		}
	}
	return &pgranger.FanOutContext{
		DBURIToDBRunnable: dbURIToDBRunnable,
	}, nil
}

func (p *EventIDFanOutProcessor) CreateFanOutContext(
	dbURIs []string,
	dbURIToShardingFieldVals map[string][][]byte,
	dbURIToDBCtrler map[string]pgranger.DBController,
) (*pgranger.FanOutContext, error) {
	panic("no need to implement when input is not sharding key.")
}

type EventIDRunnableImpl struct {
	dbCtrler          pgranger.DBController
	eventUUIDsInBytes [][]byte
}

func (r *EventIDRunnableImpl) Run(resChan chan interface{}, errChan chan error) {
	eventUUIDs := make([]uuid.UUID, len(r.eventUUIDsInBytes))
	for i, idBytes := range r.eventUUIDsInBytes {
		id, _ := uuid.FromBytes(idBytes)
		eventUUIDs[i] = id
	}
	events := []*UserEvent{}
	query, args, err := sqlx.In("select * from user_event where id in (?);", eventUUIDs)
	if err != nil {
		resChan <- nil
		errChan <- err
		return
	}
	query = r.dbCtrler.DB().Rebind(query)
	err = r.dbCtrler.DB().Select(&events, query, args...)
	if err == sql.ErrNoRows {
		fmt.Printf("EventIDRunnableImpl: no events found on shard: %s\n", r.dbCtrler.URI())
	} else if err != nil {
		resChan <- nil
		errChan <- err
		return
	} else {
		fmt.Printf("EventIDRunnableImpl: events %v found on shard: %s\n", events, r.dbCtrler.URI())
	}
	resChan <- nil
	errChan <- nil
	return
}
