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
	logger         *log.Logger
	reconnectAfter time.Duration
}

// NewFDBStreamManager - create new manager for fdb stream
func NewFDBStreamManager(client *FDBStreamClient, reconnectAfter string, logger *log.Logger) FDBStreamManager {
	reconnectDuration, err := time.ParseDuration(reconnectAfter)
	if err != nil {
		logger.Println("fdbStreamClient error: unable to parse timeout duration")
		reconnectDuration = time.Second * 3
	}

	return FDBStreamManager{client: client, logger: logger, reconnectAfter: reconnectDuration}
}

func (f *FDBStreamManager) getOplogCursor(
	client *mongo.Client,
	after, before int64,
	o *Options) (*mongo.Cursor, error) {
	query := bson.M{
		"ts":          bson.M{"$gt": after, "$lte": before},
		"op":          bson.M{"$in": opCodes},
		"fromMigrate": bson.M{"$exists": false},
	}
	opts := &options.FindOptions{}
	opts.SetSort(bson.M{"$natural": 1})
	opts.SetCursorType(options.TailableAwait)
	collection := OpLogCollection(client, o)
	return collection.Find(context.Background(), query, opts)
}

// Stop - stop event loop
func (f *FDBStreamManager) Stop() {
	f.client.Stop()
}

func (f *FDBStreamManager) fetchOplog(
	ctx context.Context,
	client *mongo.Client,
	options *Options,
	tsFrom, tsTo int64,
	output chan OpLog,
) error {
	f.logger.Println("fdbStreamManager: resumed from:", tsFrom)
	f.logger.Println("fdbStreamManager: resumed to:", tsTo)

OuterLoop:
	for {
		next := false
		cursor, err := f.getOplogCursor(client, tsFrom, tsTo, options)
		if err != nil {
			return err
		}

	InnerLoop:
		for cursor.Next(ctx) {
			var entry OpLog
			if err = cursor.Decode(&entry); err != nil {
				next = true
				break InnerLoop
			}
			output <- entry
		}
		if err = cursor.Close(context.Background()); err != nil {
			return err
		}

		if !next {
			break OuterLoop
		}
	}

	return nil
}

// Resume - resume from timestamp
func (f *FDBStreamManager) Resume(ctx context.Context, client *mongo.Client, options *Options, ts int64) chan OpLog {
	oplog := make(chan OpLog)
	bufferCh := make(chan OpLog)
	firstDoc := true
	var fdMtx sync.RWMutex

	fnSetFirstDoc := func(isFirst bool) {
		fdMtx.Lock()
		firstDoc = isFirst
		fdMtx.Unlock()
	}

	fnIsFirstDoc := func() bool {
		fdMtx.RLock()
		defer fdMtx.RUnlock()

		return firstDoc
	}

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

	fnListenBuffered := func() {
		for msg := range bufferCh {
			if fnIsFirstDoc() {
				fnSetFirstDoc(false)
				if err := f.fetchOplog(ctx, client, options, ts, msg.Timestamp, oplog); err != nil {
					f.logger.Println("fdbStreamManager (fetching ops error):", err)
				}
			}
			ts = msg.Timestamp
			oplog <- msg
		}
		close(oplog)
	}

	fnListenChanges := func() {
		for {
			fnSetFirstDoc(true)
			if err := f.client.Listen(fnProcessor); err != nil {
				f.logger.Println("fdbStreamManager (resume error):", err)

				if err == ErrStopped {
					close(bufferCh)
					return
				}
			}
			time.Sleep(f.reconnectAfter)
		}
	}

	go fnListenChanges()
	go fnListenBuffered()

	return oplog
}
