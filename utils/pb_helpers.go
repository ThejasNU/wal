package utils

import (
	"fmt"

	"github.com/ThejasNU/wal/types"
	"google.golang.org/protobuf/proto"
)

func mustMarshal(entry *types.WAL_Entry) []byte {
	serializedEntry, err := proto.Marshal(entry)
	
	if err!=nil{
		// marshal should never fail, if it fails it will be becuase of wrong protobuf definition or whole codebase
		panic(fmt.Sprintf("marshal failed: %v", err))
	}

	return serializedEntry
}

func mustUnmarshal(data []byte, entry *types.WAL_Entry){
	err:= proto.Unmarshal(data, entry)

	if err!=nil{
		// similar to marshal, unmarshal should never fail too
		panic(fmt.Sprintf("unmarshal failed: %v", err))
	}
}
