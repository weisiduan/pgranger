package pgranger

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/pkg/errors"
)

const (
	invalidHexBoundary      = "Invalid hex boundary %s"
	adjacentBeginEndUnmatch = "range %d begin does not match previous end"
	firstRangeBeginNotNil   = "first range lower boundary not nil"
	lastRangeEndNotNil      = "last range upper boundary not nil"
	flippedBoundary         = "begin %s ge than end %s"
)

// Range provides begin and end of a range and the db stores. Each range represents a micro-partition that can be moved
// across shards as one unit. It would make sense to split the sharding key space into a reasonably large amount of
// ranges.
type Range struct {
	// When range is the lower most range in the key space, begin is nil
	begin []byte
	// When range is the upper most range in the key space, end is nil
	end []byte
	// primaryDBController provides the db client and control around the db client, e.g. circuit breaker.
	primaryDBController DBController
	// replicaDBControllers provides the db clients and control around the db client, e.g. circuit breaker, for all
	// secondary read-only dbs.
	replicaDBControllers []DBController
}

func (r *Range) contains(shardingKey []byte) bool {
	return bytes.Compare(shardingKey, r.begin) >= 0 && (r.end == nil || bytes.Compare(shardingKey, r.end) < 0)
}

func (r *Range) String() string {
	return fmt.Sprintf("range[begin: %s end: %s]", string(r.begin), string(r.end))
}

// Namespace provides the ranges and corresponding db connections for a sharding key.
type Namespace struct {
	shardingFieldName string
	ranges            []*Range
}

func buildNamespaces(
	shardingConfig *ShardingConfig,
	dbURIToDBCtrler map[string]DBController,
) (map[string]*Namespace, error) {
	if shardingConfig == nil {
		return nil, errors.New("shardingConfig is nil")
	}
	keyToNamespace := make(map[string]*Namespace)
	for shardingFieldName, namespaceConfig := range shardingConfig.Namespaces {
		namespace, err := buildNamespace(shardingFieldName, namespaceConfig, dbURIToDBCtrler)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to build sharding key namespace from config")
		}
		keyToNamespace[shardingFieldName] = namespace
	}
	return keyToNamespace, nil
}

func buildNamespace(
	shardingFieldName string,
	namespaceConfig *NamespaceConfig,
	dbURIToDBCtrler map[string]DBController,
) (*Namespace, error) {
	if namespaceConfig == nil {
		return nil, errors.New("namespaceConfig is nil")
	}
	ranges := make([]*Range, len(namespaceConfig.Ranges))
	for idx, rangeConfig := range namespaceConfig.Ranges {
		if rangeConfig == nil {
			return nil, errors.New("rangeConfig is nil")
		}
		begin, end, err := buildKeyRange(rangeConfig)
		if err != nil {
			return nil, err
		}
		primaryDBCtrler, err := findDBController(dbURIToDBCtrler, rangeConfig.PrimaryDB)
		if err != nil {
			return nil, errors.WithMessage(err, "find primary db: ")
		}
		// Note replicaDBControllers can be nil as we intend to keep replicas as optional.
		replicaDBCtrlers, err := findDBControllers(dbURIToDBCtrler, rangeConfig.ReplicaDBs)
		if err != nil {
			return nil, errors.WithMessage(err, "find replica dbs: ")
		}
		ranges[idx] = &Range{
			begin:                begin,
			end:                  end,
			primaryDBController:  primaryDBCtrler,
			replicaDBControllers: replicaDBCtrlers,
		}
	}
	if err := validateRanges(ranges); err != nil {
		return nil, err
	}
	return &Namespace{shardingFieldName: shardingFieldName, ranges: ranges}, nil
}

func buildKeyRange(rangeConfig *RangeConfig) ([]byte, []byte, error) {
	begin, err := buildRangeBoundary(rangeConfig.Begin)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed to parse range %s", rangeConfig)
	}

	end, err := buildRangeBoundary(rangeConfig.End)
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed to parse range %s", rangeConfig)
	}

	if begin != nil && end != nil && bytes.Compare(begin, end) >= 0 {
		return nil, nil, errors.Errorf(flippedBoundary, hex.EncodeToString(begin), hex.EncodeToString(end))
	}

	return begin, end, nil
}

func buildRangeBoundary(boundary *string) ([]byte, error) {
	// When boundary is nil it means to be lower or upper boundary of the space of the sharding key.
	if boundary == nil {
		return nil, nil
	}
	boundaryBytes, err := hex.DecodeString(*boundary)
	if err != nil {
		return nil, errors.Errorf(invalidHexBoundary, *boundary)
	}
	return boundaryBytes, nil
}

func validateRanges(ranges []*Range) error {
	for idx, shardingRange := range ranges {
		if shardingRange == nil {
			return errors.New("range not filled")
		}
		if idx == 0 && shardingRange.begin != nil {
			return errors.New(firstRangeBeginNotNil)
		}
		if idx == len(ranges)-1 && shardingRange.end != nil {
			return errors.New(lastRangeEndNotNil)
		}
		if idx > 0 && bytes.Compare(ranges[idx-1].end, shardingRange.begin) != 0 {
			return errors.Errorf(adjacentBeginEndUnmatch, idx)
		}
	}
	return nil
}
