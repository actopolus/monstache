package gtm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"sync"
	"time"
)

// FDBStreamManager - manager for stream listener
type FDBStreamManager struct {
	client         *FDBStreamClient
	oplog          chan OpLog
	logger         *log.Logger
	fdMtx          sync.RWMutex
	reconnectAfter time.Duration
}

// NewFDBStreamManager - create new manager for fdb stream
func NewFDBStreamManager(client *FDBStreamClient, reconnectAfter string, logger *log.Logger) FDBStreamManager {
	reconnectDuration, err := time.ParseDuration(reconnectAfter)
	if err != nil {
		logger.Println("fdbStreamClient error: unable to parse timeout duration")
		reconnectDuration = time.Second * 3
	}

	return FDBStreamManager{client: client, oplog: make(chan OpLog), logger: logger, reconnectAfter: reconnectDuration}
}

func (f *FDBStreamManager) getOplogCursor(
	client *mongo.Client,
	after, before int64,
	o *Options) (*mongo.Cursor, error) {
	query := bson.M{
		"ts":          bson.M{"$gt": after, "$lt": before},
		"op":          bson.M{"$in": opCodes},
		"fromMigrate": bson.M{"$exists": false},
	}
	opts := &options.FindOptions{}
	opts.SetSort(bson.M{"$natural": 1})
	opts.SetCursorType(options.TailableAwait)
	//opts.SetOplogReplay(true)
	//opts.SetNoCursorTimeout(true)
	collection := OpLogCollection(client, o)
	return collection.Find(context.Background(), query, opts)
}

// Stop - stop event loop
func (f *FDBStreamManager) Stop() {
	f.Stop()
}

// Resume - resume from timestamp
func (f *FDBStreamManager) Resume(client *mongo.Client, options *Options, ts int64) chan OpLog {
	f.oplog = make(chan OpLog)
	bufferCh := make(chan OpLog)
	startCh := make(chan struct{})
	tsUntilCh := make(chan int64)
	firstDoc := true

	fnProcessor := func(msg []byte) error {
		docReader := bsonrw.NewBSONDocumentReader(msg)
		docDecoder, err := bson.NewDecoder(docReader)
		if err != nil {
			return err
		}

		var doc OpLog
		err = docDecoder.Decode(&doc)
		if err != nil {
			return err
		}

		bufferCh <- doc
		return nil
	}

	fnFetchOplog := func() {
		for tsUntil := range tsUntilCh {
			f.logger.Println("fdbStreamManager: resumed from:", ts)
			f.logger.Println("fdbStreamManager: resumed to:", tsUntil)

			for {
				next := false
				cursor, err := f.getOplogCursor(client, ts, tsUntil, options)
				if err != nil {
					f.logger.Println("fdbStreamManager (fetch oplog):", err)
					continue
				}

				for cursor.Next(context.Background()) {
					var entry OpLog
					if err = cursor.Decode(&entry); err != nil {
						next = true
						break
					}
					f.oplog <- entry
				}

				if next {
					continue
				}

				startCh <- struct{}{}
				break
			}
		}
	}

	fnListenBuffered := func() {
		for msg := range bufferCh {
			f.fdMtx.RLock()
			if firstDoc {
				f.fdMtx.RUnlock()

				f.fdMtx.Lock()
				firstDoc = false
				f.fdMtx.Unlock()

				tsUntilCh <- msg.Timestamp
				<-startCh
			} else {
				f.fdMtx.RUnlock()
			}

			ts = msg.Timestamp
			f.oplog <- msg
		}
	}

	fnListenChanges := func() {
		for {
			f.fdMtx.Lock()
			firstDoc = true
			f.fdMtx.Unlock()

			if err := f.client.Listen(fnProcessor); err != nil {
				f.logger.Println("fdbStreamManager (resume error):", err)

				if err == ErrStopped {
					close(tsUntilCh)
					close(startCh)
					close(bufferCh)
					close(f.oplog)
					return
				}
			}
			time.Sleep(f.reconnectAfter)
		}
	}

	go fnListenChanges()
	go fnListenBuffered()
	go fnFetchOplog()

	return f.oplog
}
