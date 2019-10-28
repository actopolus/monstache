package gtm

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/serialx/hashring"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/network/connection"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var opCodes = [...]string{"c", "i", "u", "d"}

type OrderingGuarantee int

type errchk interface {
	Err() error
}

type task struct {
	doneC  chan bool
	stopC  chan bool
	ctx    context.Context
	cancel context.CancelFunc
}

func newTask(stopC chan bool) *task {
	ctx, cancel := context.WithCancel(context.Background())
	t := &task{
		stopC:  stopC,
		doneC:  make(chan bool),
		ctx:    ctx,
		cancel: cancel,
	}
	go t.start()
	return t
}

func (t *task) start() {
	defer t.cancel()
	select {
	case <-t.doneC:
		break
	case <-t.stopC:
		break
	}
}

func (t *task) Done() {
	close(t.doneC)
}

const (
	Oplog     OrderingGuarantee = iota // ops sent in oplog order (strong ordering)
	Namespace                          // ops sent in oplog order within a namespace
	Document                           // ops sent in oplog order for a single document
	AnyOrder                           // ops sent as they become available
)

type QuerySource int

const (
	OplogQuerySource QuerySource = iota
	DirectQuerySource
)

type Options struct {
	After               TimestampGenerator
	Filter              OpFilter
	NamespaceFilter     OpFilter
	OpLogDisabled       bool
	OpLogDatabaseName   string
	OpLogCollectionName string
	ChannelSize         int
	BufferSize          int
	BufferDuration      time.Duration
	Ordering            OrderingGuarantee
	WorkerCount         int
	MaxWaitSecs         int
	UpdateDataAsDelta   bool
	ChangeStreamNs      []string
	DirectReadNs        []string
	DirectReadFilter    OpFilter
	DirectReadSplitMax  int32
	DirectReadConcur    int
	DirectReadNoTimeout bool
	Unmarshal           DataUnmarshaller
	Pipe                PipelineBuilder
	PipeAllowDisk       bool
	Log                 *log.Logger
	FetchDocs           bool
}

type Op struct {
	Id                interface{}            `json:"_id"`
	Operation         string                 `json:"operation"`
	Namespace         string                 `json:"namespace"`
	Data              map[string]interface{} `json:"data,omitempty"`
	Timestamp         int64                  `json:"timestamp"`
	Source            QuerySource            `json:"source"`
	Doc               interface{}            `json:"doc,omitempty"`
	UpdateDescription map[string]interface{} `json:"updateDescription,omitempty"`
}

type OpLog struct {
	Timestamp    int64                  "ts"
	HistoryID    int64                  "h"
	MongoVersion int                    "v"
	Operation    string                 "op"
	Namespace    string                 "ns"
	Doc          map[string]interface{} "o"
	Update       map[string]interface{} "o2"
}

type ChangeDocNs struct {
	Database   string "db"
	Collection string "coll"
}

type ChangeDoc struct {
	DocKey            map[string]interface{} "documentKey"
	Id                interface{}            "_id"
	Operation         string                 "operationType"
	FullDoc           map[string]interface{} "fullDocument"
	Namespace         ChangeDocNs            "ns"
	Timestamp         int64                  "clusterTime"
	UpdateDescription map[string]interface{} "updateDescription"
}

func (cd *ChangeDoc) docId() interface{} {
	return cd.DocKey["_id"]
}

func (cd *ChangeDoc) mapTimestamp() int64 {
	if cd.Timestamp > 0 {
		// only supported in version 4.0
		return cd.Timestamp
	} else {
		// for versions prior to 4.0 simulate a timestamp
		now := time.Now().UTC()
		return now.Unix()
	}
}

func (cd *ChangeDoc) mapOperation() string {
	if cd.Operation == "insert" {
		return "i"
	} else if cd.Operation == "update" || cd.Operation == "replace" {
		return "u"
	} else if cd.Operation == "delete" {
		return "d"
	} else if cd.Operation == "invalidate" || cd.Operation == "drop" || cd.Operation == "dropDatabase" {
		return "c"
	} else {
		return ""
	}
}

func (cd *ChangeDoc) hasUpdate() bool {
	return cd.UpdateDescription != nil
}

func (cd *ChangeDoc) hasDoc() bool {
	return (cd.mapOperation() == "i" || cd.mapOperation() == "u") && cd.FullDoc != nil
}

func (cd *ChangeDoc) isInvalidate() bool {
	return cd.Operation == "invalidate"
}

func (cd *ChangeDoc) isDrop() bool {
	return cd.Operation == "drop"
}

func (cd *ChangeDoc) isDropDatabase() bool {
	return cd.Operation == "dropDatabase"
}

func (cd *ChangeDoc) mapNs() string {
	if cd.Namespace.Collection != "" {
		return cd.Namespace.Database + "." + cd.Namespace.Collection
	} else {
		return cd.Namespace.Database + ".cmd"
	}
}

type Doc struct {
	Id interface{} "_id"
}

type CollectionStats struct {
	Count         int32 "count"
	AvgObjectSize int32 "avgObjSize"
}

type CollectionSegment struct {
	min         interface{}
	max         interface{}
	splitKey    string
	splits      []map[string]interface{}
	subSegments []*CollectionSegment
}

func (cs *CollectionSegment) shrinkTo(next interface{}) {
	cs.max = next
}

func (cs *CollectionSegment) toSelector() bson.M {
	sel, doc := bson.M{}, bson.M{}
	if cs.min != nil {
		doc["$gte"] = cs.min
	}
	if cs.max != nil {
		doc["$lt"] = cs.max
	}
	if len(doc) > 0 {
		sel[cs.splitKey] = doc
	}
	return sel
}

func (cs *CollectionSegment) divide() {
	if len(cs.splits) == 0 {
		return
	}
	ns := &CollectionSegment{
		splitKey: cs.splitKey,
		min:      cs.min,
		max:      cs.max,
	}
	cs.subSegments = nil
	for _, split := range cs.splits {
		ns.shrinkTo(split[cs.splitKey])
		cs.subSegments = append(cs.subSegments, ns)
		ns = &CollectionSegment{
			splitKey: cs.splitKey,
			min:      ns.max,
			max:      cs.max,
		}
	}
	ns = &CollectionSegment{
		splitKey: cs.splitKey,
		min:      cs.splits[len(cs.splits)-1][cs.splitKey],
	}
	cs.subSegments = append(cs.subSegments, ns)
}

func (cs *CollectionSegment) init(c *mongo.Collection) (err error) {
	opts := &options.FindOneOptions{}
	opts.SetSort(bson.M{cs.splitKey: 1})
	doc := make(map[string]interface{})
	if err = c.FindOne(context.Background(), nil, opts).Decode(&doc); err != nil {
		return
	}
	cs.min = doc[cs.splitKey]
	opts = &options.FindOneOptions{}
	opts.SetSort(bson.M{cs.splitKey: -1})
	doc = make(map[string]interface{})
	if err = c.FindOne(context.Background(), nil, opts).Decode(&doc); err != nil {
		return
	}
	cs.max = doc[cs.splitKey]
	return
}

type OpChan chan *Op

type OpLogEntry map[string]interface{}

type OpFilter func(*Op) bool

type ShardInsertHandler func(*ShardInfo) (*mongo.Client, error)

type TimestampGenerator func(*mongo.Client, *Options) (int64, error)

type DataUnmarshaller func(namespace string, data []byte) (interface{}, error)

type PipelineBuilder func(namespace string, changeStream bool) ([]interface{}, error)

type OpBuf struct {
	Entries        []*Op
	BufferSize     int
	BufferDuration time.Duration
}

type OpCtx struct {
	lock             *sync.Mutex
	OpC              OpChan
	ErrC             chan error
	DirectReadWg     *sync.WaitGroup
	directReadConcWg *sync.WaitGroup
	stopC            chan bool
	allWg            *sync.WaitGroup
	seekC            chan int64
	pauseC           chan bool
	resumeC          chan bool
	paused           bool
	stopped          bool
	log              *log.Logger
}

type OpCtxMulti struct {
	lock         *sync.Mutex
	contexts     []*OpCtx
	OpC          OpChan
	ErrC         chan error
	DirectReadWg *sync.WaitGroup
	opWg         *sync.WaitGroup
	stopC        chan bool
	allWg        *sync.WaitGroup
	seekC        chan primitive.Timestamp
	pauseC       chan bool
	resumeC      chan bool
	paused       bool
	stopped      bool
	log          *log.Logger
}

type ShardInfo struct {
	hostname string
}

type N struct {
	database   string
	collection string
}

func (n *N) parse(ns string) (err error) {
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) != 2 {
		err = fmt.Errorf("Invalid ns: %s :expecting db.collection", ns)
	} else {
		n.database = parts[0]
		n.collection = parts[1]
	}
	return
}

func (n *N) parseForChanges(ns string) {
	if ns == "" {
		// watch the whole deployment
		n.database = ""
		n.collection = ""
		return
	}
	parts := strings.SplitN(ns, ".", 2)
	if len(parts) == 1 {
		n.database = parts[0]
		n.collection = ""
	} else {
		n.database = parts[0]
		n.collection = parts[1]
	}
	return
}

func (n *N) desc() (dsc string) {
	if n.isDatabase() {
		dsc = fmt.Sprintf("database %s", n.database)
	} else if n.isCollection() {
		dsc = fmt.Sprintf("collection %s.%s", n.database, n.collection)
	} else {
		dsc = "the deployment"
	}
	return
}

func (n *N) isDeployment() bool {
	return n.database == "" && n.collection == ""
}

func (n *N) isDatabase() bool {
	return n.database != "" && n.collection == ""
}

func (n *N) isCollection() bool {
	return n.database != "" && n.collection != ""
}

func (shard *ShardInfo) GetURL() string {
	hostParts := strings.SplitN(shard.hostname, "/", 2)
	if len(hostParts) == 2 {
		return "mongodb://" + hostParts[1] + "?replicaSet=" + hostParts[0]
	} else {
		return "mongodb://" + hostParts[0]
	}
}

func (ctx *OpCtx) isStopped() bool {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	return ctx.stopped
}

func (ctx *OpCtx) Since(ts int64) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.seekC <- ts
}

func (ctx *OpCtx) Pause() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.paused {
		ctx.paused = true
		ctx.pauseC <- true
	}
}

func (ctx *OpCtx) Resume() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.paused {
		ctx.paused = false
		ctx.resumeC <- true
	}
}

func (ctx *OpCtx) Stop() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.stopped {
		ctx.stopped = true
		close(ctx.stopC)
		ctx.allWg.Wait()
		close(ctx.OpC)
		close(ctx.ErrC)
	}
}

func (ctx *OpCtxMulti) Since(ts int64) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	for _, child := range ctx.contexts {
		child.Since(ts)
	}
}

func (ctx *OpCtxMulti) Pause() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.paused {
		ctx.paused = true
		ctx.pauseC <- true
		for _, child := range ctx.contexts {
			child.Pause()
		}
	}
}

func (ctx *OpCtxMulti) Resume() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if ctx.paused {
		ctx.paused = false
		ctx.resumeC <- true
		for _, child := range ctx.contexts {
			child.Resume()
		}
	}
}

func (ctx *OpCtxMulti) Stop() {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	if !ctx.stopped {
		ctx.stopped = true
		close(ctx.stopC)
		for _, c := range ctx.contexts {
			child := c
			go child.Stop()
		}
		ctx.allWg.Wait()
		ctx.opWg.Wait()
		close(ctx.OpC)
		close(ctx.ErrC)
	}
}

func unwrapErr(err error) error {
	if err == nil {
		return nil
	}
	if ce, ok := err.(connection.Error); ok {
		if ce.Wrapped != nil {
			return unwrapErr(ce.Wrapped)
		}
	}
	return err
}

func ChainOpFilters(filters ...OpFilter) OpFilter {
	return func(op *Op) bool {
		for _, filter := range filters {
			if filter(op) == false {
				return false
			}
		}
		return true
	}
}

func (this *Op) IsDrop() bool {
	if _, drop := this.IsDropDatabase(); drop {
		return true
	}
	if _, drop := this.IsDropCollection(); drop {
		return true
	}
	return false
}

func (this *Op) IsDropCollection() (string, bool) {
	if this.IsCommand() {
		if this.Data != nil {
			if val, ok := this.Data["drop"]; ok {
				return val.(string), true
			}
		}
	}
	return "", false
}

func (this *Op) IsDropDatabase() (string, bool) {
	if this.IsCommand() {
		if this.Data != nil {
			if _, ok := this.Data["dropDatabase"]; ok {
				return this.GetDatabase(), true
			}
		}
	}
	return "", false
}

func (this *Op) IsCommand() bool {
	return this.Operation == "c"
}

func (this *Op) IsInsert() bool {
	return this.Operation == "i"
}

func (this *Op) IsUpdate() bool {
	return this.Operation == "u"
}

func (this *Op) IsDelete() bool {
	return this.Operation == "d"
}

func (this *Op) IsSourceOplog() bool {
	return this.Source == OplogQuerySource
}

func (this *Op) IsSourceDirect() bool {
	return this.Source == DirectQuerySource
}

func (this *Op) ParseNamespace() []string {
	return strings.SplitN(this.Namespace, ".", 2)
}

func (this *Op) GetDatabase() string {
	return this.ParseNamespace()[0]
}

func (this *Op) GetCollection() string {
	if _, drop := this.IsDropDatabase(); drop {
		return ""
	} else if col, drop := this.IsDropCollection(); drop {
		return col
	} else {
		return this.ParseNamespace()[1]
	}
}

func (this *OpBuf) Append(op *Op) {
	this.Entries = append(this.Entries, op)
}

func (this *OpBuf) IsFull() bool {
	return len(this.Entries) >= this.BufferSize
}

func (this *OpBuf) HasOne() bool {
	return len(this.Entries) == 1
}

func (this *OpBuf) Flush(client *mongo.Client, ctx *OpCtx, o *Options) {
	if len(this.Entries) == 0 {
		return
	}
	ns := make(map[string][]interface{})
	byId := make(map[interface{}][]*Op)
	for _, op := range this.Entries {
		if op.IsUpdate() && op.Doc == nil {
			oId, err := primitive.ObjectIDFromHex(op.Id.(string))
			if err != nil {
				ctx.ErrC <- errors.Wrap(err, "Error convert Id to ObjectId")
				continue
			}
			ns[op.Namespace] = append(ns[op.Namespace], oId)
			idKey := fmt.Sprintf("%s.%v", op.Namespace, oId)
			byId[idKey] = append(byId[idKey], op)
		}
	}
retry:
	for n, opIds := range ns {
		var parts = strings.SplitN(n, ".", 2)
		db, col := parts[0], parts[1]
		sel := bson.M{"_id": bson.M{"$in": opIds}}
		collection := client.Database(db).Collection(col)
		cursor, err := collection.Find(context.Background(), sel)
		if err == nil {
			for cursor.Next(context.Background()) {
				doc := make(map[string]interface{})
				if err = cursor.Decode(&doc); err == nil {
					resultId := fmt.Sprintf("%s.%v", n, doc["_id"])
					if ops, ok := byId[resultId]; ok {
						for _, op := range ops {
							op.processData(doc, o)
						}
					}

				}
			}
			if err = cursor.Close(context.Background()); err != nil {
				ctx.ErrC <- errors.Wrap(err, "Error finding documents to associate with ops")
			}
		} else {
			ctx.ErrC <- errors.Wrap(err, "Error finding documents to associate with ops")
			break retry
		}
	}
	for _, op := range this.Entries {
		if op.matchesFilter(o) {
			ctx.OpC <- op
		}
	}
	this.Entries = nil
}

func UpdateIsReplace(entry map[string]interface{}) bool {
	if _, ok := entry["set"]; ok {
		return false
	} else if _, ok := entry["unset"]; ok {
		return false
	} else {
		return true
	}
}

func (this *Op) shouldParse() bool {
	return this.IsInsert() || this.IsDelete() || this.IsUpdate() || this.IsCommand()
}

func (this *Op) matchesNsFilter(o *Options) bool {
	return o.NamespaceFilter == nil || o.NamespaceFilter(this)
}

func (this *Op) matchesFilter(o *Options) bool {
	return o.Filter == nil || o.Filter(this)
}

func (this *Op) matchesDirectFilter(o *Options) bool {
	return o.DirectReadFilter == nil || o.DirectReadFilter(this)
}

func normalizeDocSlice(a []interface{}) []interface{} {
	var avs []interface{}
	for _, av := range a {
		var avc interface{}
		switch achild := av.(type) {
		case map[string]interface{}:
			avc = normalizeDocMap(achild)
		case primitive.M:
			avc = normalizeDocMap(map[string]interface{}(achild))
		case primitive.D:
			avc = normalizeDocMap(map[string]interface{}(achild.Map()))
		case []interface{}:
			avc = normalizeDocSlice(achild)
		case primitive.A:
			avc = normalizeDocSlice([]interface{}(achild))
		default:
			avc = av
		}
		avs = append(avs, avc)
	}
	return avs
}

func normalizeDocMap(m map[string]interface{}) map[string]interface{} {
	o := map[string]interface{}{}
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			o[k] = normalizeDocMap(child)
		case primitive.M:
			o[k] = normalizeDocMap(map[string]interface{}(child))
		case primitive.D:
			o[k] = normalizeDocMap(map[string]interface{}(child.Map()))
		case []interface{}:
			o[k] = normalizeDocSlice(child)
		case primitive.A:
			o[k] = normalizeDocSlice([]interface{}(child))
		default:
			o[k] = v
		}
	}
	return o
}

func (this *Op) processData(data interface{}, o *Options) {
	if data != nil {
		this.Doc = data
		if m, ok := data.(map[string]interface{}); ok {
			this.Data = normalizeDocMap(m)
			this.Doc = this.Data
		}
		if o.Unmarshal != nil {
			this.processDoc(data, o)
		}
	}
}

func (this *Op) processDoc(data interface{}, o *Options) {
	if o.Unmarshal == nil || data == nil {
		return
	}
	b, err := bson.Marshal(data)
	if err == nil {
		this.Doc, err = o.Unmarshal(this.Namespace, b)
		if err != nil {
			o.Log.Printf("Unable to process document: %s", err)
		}
	} else {
		o.Log.Printf("Unable to process document: %s", err)
	}
	return
}

func (this *Op) ParseLogEntry(entry *OpLog, o *Options) (include bool, err error) {
	var rawField map[string]interface{}
	this.Operation = entry.Operation
	this.Timestamp = entry.Timestamp
	this.Namespace = entry.Namespace
	if this.shouldParse() {
		if this.IsCommand() {
			rawField = entry.Doc
			this.processData(rawField, o)
		}
		if this.matchesNsFilter(o) {
			if this.IsInsert() || this.IsDelete() || this.IsUpdate() {
				if this.IsUpdate() {
					rawField = entry.Update
				} else {
					rawField = entry.Doc
				}
				this.Id = rawField["_id"]
				if this.IsInsert() {
					this.processData(rawField, o)
				} else if this.IsUpdate() {
					rawField = entry.Doc
					if o.UpdateDataAsDelta || UpdateIsReplace(rawField) {
						this.processData(rawField, o)
					} else if !o.FetchDocs {
						this.processData(this.parseOplogChange(rawField), o)
					}
				}
				include = true
			} else if this.IsCommand() {
				include = this.IsDrop()
			}
		}
	}
	return
}

func (this *Op) parseOplogChange(m map[string]interface{}) interface{} {
	if !UpdateIsReplace(m) {
		if setmap, ok := m["set"]; ok {
			return setmap
		}
	}
	return nil
}

func OpLogCollection(client *mongo.Client, o *Options) *mongo.Collection {
	localDB := client.Database(o.OpLogDatabaseName)
	return localDB.Collection(o.OpLogCollectionName)
}

func validOps() bson.M {
	return bson.M{"op": bson.M{"$in": opCodes}}
}

func LastOpTimestamp(client *mongo.Client, o *Options) (int64, error) {
	opLog := OpLog{}
	filter := validOps()
	opts := &options.FindOneOptions{}
	opts.SetSort(bson.M{"$natural": -1})
	c := OpLogCollection(client, o)
	err := c.FindOne(context.Background(), filter, opts).Decode(&opLog)
	return opLog.Timestamp, err
}

func opDataReady(op *Op, o *Options) (ready bool) {
	if o.UpdateDataAsDelta {
		ready = true
	} else if o.Ordering == AnyOrder {
		if op.IsUpdate() {
			ready = op.Data != nil || op.Doc != nil
		} else {
			ready = true
		}
	}
	return
}

func TailFDBOps(ctx *OpCtx, client *mongo.Client, fdb *FDBStreamManager, channels []OpChan, o *Options) error {
	defer ctx.allWg.Done()
	var cts int64

	if o.After != nil {
		cts, _ = o.After(client, o)
	} else {
		cts, _ = LastOpTimestamp(client, o)
	}

	task := newTask(ctx.stopC)
	defer task.Done()
	for task.ctx.Err() == nil {
		msgs := fdb.Resume(task.ctx, client, o, cts)
	MsgStream:
		for {
			var (
				entry OpLog
				ok    bool
			)

			select {
			case entry, ok = <-msgs:
				if !ok {
					break MsgStream
				}
			case <-task.ctx.Done():
				fdb.Stop()
				break MsgStream
			}
			op := &Op{Id: "", Operation: "", Namespace: "", Data: nil, Timestamp: 0, Source: OplogQuerySource}
			ok, err := op.ParseLogEntry(&entry, o)
			if err == nil {
				if ok && op.matchesFilter(o) {
					if opDataReady(op, o) {
						ctx.OpC <- op
					} else {
						// broadcast to fetch channels
						for _, channel := range channels {
							channel <- op
						}
					}
				}
			} else {
				ctx.ErrC <- errors.Wrap(err, "Error parsing the oplog document")
			}
			select {
			case ts := <-ctx.seekC:
				cts = ts
				fdb.Stop()
				break MsgStream
			case <-ctx.pauseC:
				fdb.Stop()
				<-ctx.resumeC
				select {
				case ts := <-ctx.seekC:
					cts = ts
				default:
				}
				break MsgStream
			default:
				cts = op.Timestamp
			}
		}
	}

	return nil
}

func DirectReadSegment(ctx *OpCtx, client *mongo.Client, ns string, o *Options, seg *CollectionSegment) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	defer ctx.directReadConcWg.Done()
	task := newTask(ctx.stopC)
	defer task.Done()
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error starting direct reads. Invalid namespace.")
		return
	}

	c := client.Database(n.database).Collection(n.collection)
	sel := seg.toSelector()

	opts := options.Find()
	opts.SetBatchSize(500)
	opts.SetNoCursorTimeout(true)

	cursor, err := c.Find(task.ctx, sel, opts)
	if err != nil {
		ctx.ErrC <- errors.Wrap(err, fmt.Sprintf("Error performing direct read of collection %s", ns))
		return
	}

	result := map[string]interface{}{}
	for cursor.Next(task.ctx) {
		if err = cursor.Decode(&result); err != nil {
			ctx.ErrC <- errors.Wrap(err, "Error decoding cursor in direct reads")
			result = map[string]interface{}{}
			continue
		}
		t := time.Now().UTC().Unix()
		op := &Op{
			Id:        result["_id"],
			Operation: "i",
			Namespace: ns,
			Source:    DirectQuerySource,
			Timestamp: t,
		}
		op.processData(result, o)
		if op.matchesDirectFilter(o) {
			ctx.OpC <- op
		}
		result = map[string]interface{}{}
	}
	if err = cursor.Err(); err != nil {
		ctx.ErrC <- errors.Wrap(err, fmt.Sprintf("Error performing direct read of collection %s", ns))
	}

	cursor.Close(context.Background())
	return
}

func ProcessDirectReads(ctx *OpCtx, client *mongo.Client, o *Options) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	concur := o.DirectReadConcur
	running := 0
	for _, ns := range o.DirectReadNs {
		if concur > 0 && running >= concur {
			ctx.directReadConcWg.Wait()
			running = 0
		}
		ctx.DirectReadWg.Add(1)
		ctx.directReadConcWg.Add(1)
		ctx.allWg.Add(1)
		go DirectReadPaged(ctx, client, ns, o)
		running = running + 1
	}
	return
}

func DirectReadPaged(ctx *OpCtx, client *mongo.Client, ns string, o *Options) (err error) {
	defer ctx.allWg.Done()
	defer ctx.DirectReadWg.Done()
	defer ctx.directReadConcWg.Done()
	n := &N{}
	if err = n.parse(ns); err != nil {
		ctx.ErrC <- errors.Wrap(err, "Error starting direct reads. Invalid namespace.")
		return
	}
	segment := &CollectionSegment{splitKey: "_id"}
	ctx.allWg.Add(1)
	ctx.DirectReadWg.Add(1)
	ctx.directReadConcWg.Add(1)
	go DirectReadSegment(ctx, client, ns, o, segment)
	return
}

func FetchDocuments(ctx *OpCtx, client *mongo.Client, filter OpFilter, buf *OpBuf, inOp OpChan, o *Options) error {
	defer ctx.allWg.Done()
	timer := time.NewTimer(buf.BufferDuration)
	timer.Stop()
	for {
		select {
		case <-ctx.stopC:
			return nil
		case <-timer.C:
			buf.Flush(client, ctx, o)
		case op := <-inOp:
			if op == nil {
				break
			}
			if filter(op) {
				buf.Append(op)
				if buf.IsFull() {
					timer.Stop()
					buf.Flush(client, ctx, o)
				} else if buf.HasOne() {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(buf.BufferDuration)
				}
			}
		}
	}
	return nil
}

func OpFilterForOrdering(ordering OrderingGuarantee, workers []string, worker string) OpFilter {
	switch ordering {
	case AnyOrder, Document:
		ring := hashring.New(workers)
		return func(op *Op) bool {
			var key string
			if op.Id != nil {
				key = fmt.Sprintf("%v", op.Id)
			} else {
				key = op.Namespace
			}
			if who, ok := ring.GetNode(key); ok {
				return who == worker
			} else {
				return false
			}
		}
	case Namespace:
		ring := hashring.New(workers)
		return func(op *Op) bool {
			if who, ok := ring.GetNode(op.Namespace); ok {
				return who == worker
			} else {
				return false
			}
		}
	default:
		return func(op *Op) bool {
			return true
		}
	}
}

func DefaultOptions() *Options {
	return &Options{
		After:               LastOpTimestamp,
		Filter:              nil,
		NamespaceFilter:     nil,
		OpLogDatabaseName:   "local",
		OpLogCollectionName: "oplog.rs",
		ChannelSize:         2048,
		BufferSize:          50,
		BufferDuration:      time.Duration(75) * time.Millisecond,
		Ordering:            Oplog,
		WorkerCount:         10,
		MaxWaitSecs:         10,
		UpdateDataAsDelta:   false,
		DirectReadNs:        []string{},
		DirectReadFilter:    nil,
		DirectReadSplitMax:  9,
		DirectReadConcur:    0,
		DirectReadNoTimeout: false,
		Unmarshal:           nil,
		Log:                 log.New(os.Stdout, "INFO ", log.Flags()),
		FetchDocs:           false,
	}
}

func defaultUnmarshaller(namespace string, cursor mongo.Cursor) (interface{}, error) {
	var m map[string]interface{}
	if err := cursor.Decode(&m); err == nil {
		return m, nil
	} else {
		return nil, err
	}
}

func (this *Options) SetDefaults() {
	defaultOpts := DefaultOptions()
	if this.ChannelSize < 1 {
		this.ChannelSize = defaultOpts.ChannelSize
	}
	if this.BufferSize < 1 {
		this.BufferSize = defaultOpts.BufferSize
	}
	if this.BufferDuration == 0 {
		this.BufferDuration = defaultOpts.BufferDuration
	}
	if this.Ordering == Oplog {
		this.WorkerCount = 1
	}
	if this.WorkerCount < 1 {
		this.WorkerCount = 1
	}
	if this.UpdateDataAsDelta {
		this.Ordering = Oplog
		this.WorkerCount = 0
	}
	if this.Unmarshal == nil {
		this.Unmarshal = defaultOpts.Unmarshal
	}
	if this.Log == nil {
		this.Log = defaultOpts.Log
	}
	if this.DirectReadConcur == 0 {
		this.DirectReadConcur = defaultOpts.DirectReadConcur
	}
	if this.DirectReadSplitMax == 0 {
		this.DirectReadSplitMax = defaultOpts.DirectReadSplitMax
	}
	if len(this.ChangeStreamNs) == 0 {
		if this.After == nil {
			this.After = defaultOpts.After
		}
	} else {
		this.OpLogDisabled = true
	}
	if this.OpLogDatabaseName == "" {
		this.OpLogDatabaseName = defaultOpts.OpLogDatabaseName
	}
	if this.OpLogCollectionName == "" {
		this.OpLogCollectionName = defaultOpts.OpLogCollectionName
	}
	if this.MaxWaitSecs == 0 {
		this.MaxWaitSecs = defaultOpts.MaxWaitSecs
	}
}

func StartMulti(clients []*mongo.Client, fdbManager *FDBStreamManager, o *Options) *OpCtxMulti {
	if o == nil {
		o = DefaultOptions()
	} else {
		o.SetDefaults()
	}

	stopC := make(chan bool, 1)
	errC := make(chan error, o.ChannelSize)
	opC := make(OpChan, o.ChannelSize)

	var directReadWg sync.WaitGroup
	var opWg sync.WaitGroup
	var allWg sync.WaitGroup
	var seekC = make(chan primitive.Timestamp, 1)
	var pauseC = make(chan bool, 1)
	var resumeC = make(chan bool, 1)

	ctxMulti := &OpCtxMulti{
		lock:         &sync.Mutex{},
		OpC:          opC,
		ErrC:         errC,
		DirectReadWg: &directReadWg,
		opWg:         &opWg,
		stopC:        stopC,
		allWg:        &allWg,
		pauseC:       pauseC,
		resumeC:      resumeC,
		seekC:        seekC,
		log:          o.Log,
	}

	ctxMulti.lock.Lock()
	defer ctxMulti.lock.Unlock()

	for _, client := range clients {
		ctx := Start(client, fdbManager, o)
		ctxMulti.contexts = append(ctxMulti.contexts, ctx)
		allWg.Add(1)
		directReadWg.Add(1)
		opWg.Add(2)
		go func() {
			defer directReadWg.Done()
			ctx.DirectReadWg.Wait()
		}()
		go func() {
			defer allWg.Done()
			ctx.allWg.Wait()
		}()
		go func(c OpChan) {
			defer opWg.Done()
			for op := range c {
				opC <- op
			}
		}(ctx.OpC)
		go func(c chan error) {
			defer opWg.Done()
			for err := range c {
				errC <- err
			}
		}(ctx.ErrC)
	}
	return ctxMulti
}

func Start(client *mongo.Client, fdbManager *FDBStreamManager, o *Options) *OpCtx {
	if o == nil {
		o = DefaultOptions()
	} else {
		o.SetDefaults()
	}

	stopC := make(chan bool)
	errC := make(chan error, o.ChannelSize)
	opC := make(OpChan, o.ChannelSize)

	var inOps []OpChan
	var workerNames []string
	var directReadWg sync.WaitGroup
	var directReadConcWg sync.WaitGroup
	var allWg sync.WaitGroup

	streams := len(o.ChangeStreamNs)
	if o.OpLogDisabled == false {
		streams += 1
	}

	var seekC = make(chan int64, streams)
	var pauseC = make(chan bool, streams)
	var resumeC = make(chan bool, streams)

	ctx := &OpCtx{
		lock:             &sync.Mutex{},
		OpC:              opC,
		ErrC:             errC,
		DirectReadWg:     &directReadWg,
		directReadConcWg: &directReadConcWg,
		stopC:            stopC,
		allWg:            &allWg,
		pauseC:           pauseC,
		resumeC:          resumeC,
		seekC:            seekC,
		log:              o.Log,
	}

	if o.OpLogDisabled == false {
		for i := 1; i <= o.WorkerCount; i++ {
			workerNames = append(workerNames, strconv.Itoa(i))
		}
		for i := 1; i <= o.WorkerCount; i++ {
			allWg.Add(1)
			inOp := make(OpChan, o.ChannelSize)
			inOps = append(inOps, inOp)
			buf := &OpBuf{
				BufferSize:     o.BufferSize,
				BufferDuration: o.BufferDuration,
			}
			worker := strconv.Itoa(i)
			filter := OpFilterForOrdering(o.Ordering, workerNames, worker)
			go FetchDocuments(ctx, client, filter, buf, inOp, o)
		}
	}

	if len(o.DirectReadNs) > 0 {
		directReadWg.Add(1)
		allWg.Add(1)
		go ProcessDirectReads(ctx, client, o)
	}

	if o.OpLogDisabled == false {
		allWg.Add(1)
		go func() {
			directReadWg.Wait()
			go TailFDBOps(ctx, client, fdbManager, inOps, o)
		}()
	}

	return ctx
}
