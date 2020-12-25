package docsyncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/olivere/elastic"
	"sync/atomic"
	"time"

	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/oplog"

	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"sync"
)

var (
	GlobalCollExecutorId int32 = -1
	GlobalDocExecutorId  int32 = -1
)

type CollectionExecutor struct {
	// multi executor
	executors []*DocExecutor
	// worker id
	id int
	// mongo url
	mongoUrl string

	ns utils.NS

	wg sync.WaitGroup
	// batchCount int64

	conn *utils.MongoConn

	docBatch chan []*bson.Raw

	docBatchWithNS chan []*bsonWithNS

	// not own
	syncer   *DBSyncer
	esSyncer *ESSyncer

	esUrl string
}

func GenerateCollExecutorId() int {
	return int(atomic.AddInt32(&GlobalCollExecutorId, 1))
}

func NewCollectionExecutorToES(id int, esUrl string, ns utils.NS, esSyncer *ESSyncer) *CollectionExecutor {
	return &CollectionExecutor{
		id:       id,
		esUrl:    esUrl,
		ns:       ns,
		esSyncer: esSyncer,
		// batchCount: 0,
	}
}

func NewCollectionExecutor(id int, mongoUrl string, ns utils.NS, syncer *DBSyncer) *CollectionExecutor {
	return &CollectionExecutor{
		id:       id,
		mongoUrl: mongoUrl,
		ns:       ns,
		syncer:   syncer,
		// batchCount: 0,
	}
}

func (colExecutor *CollectionExecutor) startToSyncToMongoDB() error {
	var err error
	if colExecutor.conn, err = utils.NewMongoConn(colExecutor.mongoUrl, utils.VarMongoConnectModePrimary, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault); err != nil {
		return err
	}
	if conf.Options.FullSyncExecutorMajorityEnable {
		colExecutor.conn.Session.EnsureSafe(&mgo.Safe{WMode: utils.MajorityWriteConcern})
	}

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		docSession := colExecutor.conn.Session.Clone()
		executors[i] = NewDocExecutor(GenerateDocExecutorId(), colExecutor, docSession, colExecutor.syncer)
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

func (colExecutor *CollectionExecutor) Start() error {
	if conf.Options.Tunnel == utils.VarTunnelDirect {
		err := colExecutor.startToSyncToMongoDB()
		return err
	}
	if conf.Options.Tunnel == utils.VarTunnelMock {
		//同步到ES
		err := colExecutor.startToSyncToES()
		return err
	}
	return nil
}

//写进es的回调
func afterBulk(executionID int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response != nil && response.Errors {
		failed := response.Failed()
		if failed != nil {
			for _, item := range failed {
				if item.Status == 409 {
					// ignore version conflict since this simply means the doc
					// is already in the index
					continue
				}
				json, err := json.Marshal(item)
				if err != nil {
					conf.Eslog.Error("Unable to marshal bulk response item: %s", err)
				} else {
					conf.Eslog.Error("Bulk response item: %s", string(json))
				}
			}
		}
	} else {
		conf.Eslog.Error("ES Response %v %v", response, &response.Items)
	}
}

func NewBulkProcessor(client *elastic.Client) (bulk *elastic.BulkProcessor, err error) {
	bulkService := client.BulkProcessor().Name("mongoshake")
	bulkService.Workers(1)
	bulkService.Stats(true)
	bulkService.BulkActions(-1)
	bulkService.BulkSize(8 * 1024 * 1024)
	bulkService.Backoff(&elastic.StopBackoff{})
	bulkService.After(afterBulk)
	bulkService.FlushInterval(time.Duration(5) * time.Second)
	return bulkService.Do(context.Background())
}

//同步读的逻辑
func (colExecutor *CollectionExecutor) Sync(docs []*bson.Raw) {
	count := uint64(len(docs))
	if count == 0 {
		return
	}

	/*
	 * TODO, waitGroup.Add may overflow, so use atomic to replace waitGroup
	 * // colExecutor.wg.Add(1)
	 */
	colExecutor.wg.Add(1)
	// atomic.AddInt64(&colExecutor.batchCount, 1)
	colExecutor.docBatch <- docs
}

func (colExecutor *CollectionExecutor) Wait() error {
	colExecutor.wg.Wait()
	/*for v := atomic.LoadInt64(&colExecutor.batchCount); v != 0; {
		utils.YieldInMs(1000)
		LOG.Info("CollectionExecutor[%v %v] wait batchCount[%v] == 0", colExecutor.ns, colExecutor.id, v)
	}*/

	close(colExecutor.docBatch)
	if colExecutor.conn != nil {
		colExecutor.conn.Close()
	}

	for _, exec := range colExecutor.executors {
		if exec.error != nil {
			return errors.New(fmt.Sprintf("sync ns %v failed. %v", colExecutor.ns, exec.error))
		}
	}
	return nil
}

func (colExecutor *CollectionExecutor) StartForEs() error {

	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)
	processor, err := NewBulkProcessor(colExecutor.esSyncer.client)
	if err != nil {
		return err
	}
	executors := make([]*DocExecutor, parallel)
	for i := 0; i != len(executors); i++ {
		executors[i] = NewDocExecutorToES(GenerateDocExecutorId(), colExecutor, processor, colExecutor.esSyncer)
		go executors[i].start()
	}
	return nil
}

type bsonWithNS struct {
	doc *bson.Raw
	ns  utils.NS
}

func (colExecutor *CollectionExecutor) SyncToES(docs []*bson.Raw, ns utils.NS) {

	count := uint64(len(docs))
	if count == 0 {
		return
	}

	var bsonToWriteToEs = make([]*bsonWithNS, count)
	for i := 0; i < len(docs); i++ {
		raw := docs[i]
		withNS := &bsonWithNS{
			doc: raw,
			ns:  ns,
		}
		bsonToWriteToEs = append(bsonToWriteToEs, withNS)
	}

	/*
	 * TODO, waitGroup.Add may overflow, so use atomic to replace waitGroup
	 * // colExecutor.wg.Add(1)
	 */
	colExecutor.wg.Add(1)

	colExecutor.docBatchWithNS <- bsonToWriteToEs

}

func (colExecutor *CollectionExecutor) startToSyncToES() error {
	parallel := conf.Options.FullSyncReaderWriteDocumentParallel
	colExecutor.docBatch = make(chan []*bson.Raw, parallel)

	executors := make([]*DocExecutor, parallel)
	//fixme 这里有问题
	//此处对 ES 客户端不熟悉
	for i := 0; i != len(executors); i++ {
		//在此初始化写的executor
		processor, err := NewBulkProcessor(colExecutor.esSyncer.client)
		if err != nil {
			conf.Eslog.Error("连不上es bulk")
			return err
		}
		executors[i] = NewDocExecutorToES(GenerateDocExecutorId(), colExecutor, processor, colExecutor.esSyncer)
		go executors[i].start()
	}
	colExecutor.executors = executors
	return nil
}

type DocExecutor struct {
	// sequence index id in each replayer
	id int
	// colExecutor, not owned
	colExecutor *CollectionExecutor

	session *mgo.Session

	error error
	// not own
	syncer   *DBSyncer
	essyncer *ESSyncer

	esbulk *elastic.BulkProcessor
}

func GenerateDocExecutorId() int {
	return int(atomic.AddInt32(&GlobalDocExecutorId, 1))
}

func NewDocExecutor(id int, colExecutor *CollectionExecutor, session *mgo.Session, syncer *DBSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		session:     session,
		syncer:      syncer,
	}
}

func NewDocExecutorToES(id int, colExecutor *CollectionExecutor, bulk *elastic.BulkProcessor, syncer *ESSyncer) *DocExecutor {
	return &DocExecutor{
		id:          id,
		colExecutor: colExecutor,
		esbulk:      bulk,
		essyncer:    syncer,
	}
}

func (exec *DocExecutor) String() string {
	return fmt.Sprintf("DocExecutor[%v] collectionExecutor[%v]", exec.id, exec.colExecutor.ns)
}

func (exec *DocExecutor) start() {
	if conf.Options.Tunnel == utils.VarTunnelDirect {
		defer exec.session.Close()
	}
	for {
		docs, ok := <-exec.colExecutor.docBatch
		if !ok {
			break
		}

		if exec.error == nil {
			if err := exec.doSync(docs); err != nil {
				exec.error = err
				// since v2.4.11: panic directly if meets error
				LOG.Crashf("%s sync failed: %v", exec, err)
			}
		}

		exec.colExecutor.wg.Done()
		// atomic.AddInt64(&exec.colExecutor.batchCount, -1)
	}
}

func (exec *DocExecutor) doSync(docs []*bson.Raw) error {
	if len(docs) == 0 || conf.Options.FullSyncExecutorDebug {
		return nil
	}

	ns := exec.colExecutor.ns

	var docList []interface{}
	for _, doc := range docs {
		docList = append(docList, doc)
	}

	// qps limit if enable
	if conf.Options.Tunnel == utils.VarTunnelMock {
		if exec.essyncer.qos.Limit > 0 {
			exec.essyncer.qos.FetchBucket()
		}
	}

	if conf.Options.Tunnel == utils.VarTunnelDirect {
		if exec.syncer.qos.Limit > 0 {
			exec.syncer.qos.FetchBucket()
		}
		var docBeg, docEnd bson.M
		bson.Unmarshal(docs[0].Data, &docBeg)
		bson.Unmarshal(docs[len(docs)-1].Data, &docEnd)
		LOG.Debug("DBSyncer id[%v] doSync with table[%v] batch _id interval [%v, %v]", exec.syncer.id, ns,
			docBeg["_id"], docEnd["_id"])
	} else if conf.Options.Tunnel == utils.VarTunnelMock {
		if exec.essyncer.qos.Limit > 0 {
			exec.essyncer.qos.FetchBucket()
		}
		var docBeg, docEnd bson.M
		bson.Unmarshal(docs[0].Data, &docBeg)
		bson.Unmarshal(docs[len(docs)-1].Data, &docEnd)
		conf.Eslog.Debug("ESSyncer id[%v] doSync with table[%v] batch _id interval [%v, %v]", exec.essyncer.id, ns,
			docBeg["_id"], docEnd["_id"])
	}

	//bson.Unmarshal(docs[0].Data,&m)
	//id := m["_id"].(bson.ObjectId)
	//LOG.Debug("aaaa [%v]",id.Hex())
	if conf.Options.Tunnel == utils.VarTunnelDirect {
		collectionHandler := exec.session.DB(ns.Database).C(ns.Collection)
		bulk := collectionHandler.Bulk()
		bulk.Insert(docList...)
		if _, err := bulk.Run(); err != nil {
			LOG.Warn("insert docs with length[%v] into ns[%v] of dest mongo failed[%v]",
				len(docList), ns, err)
			index, errMsg, dup := utils.FindFirstErrorIndexAndMessage(err.Error())
			if index == -1 {
				return fmt.Errorf("bulk run failed[%v]", err)
			} else if !dup {
				var docD bson.D
				bson.Unmarshal(docs[index].Data, &docD)
				return fmt.Errorf("bulk run message[%v], failed[%v], index[%v] dup[%v]", docD, errMsg, index, dup)
			} else {
				LOG.Warn("dup error found, try to solve error")
			}

			return exec.tryOneByOne(docList, index, collectionHandler)
		}
		//写入 ES
	} else if conf.Options.Tunnel == utils.VarTunnelMock {

		for i := 0; i < len(docs); i++ {
			var m map[string]interface{}
			bson.Unmarshal(docs[i].Data, &m)
			req := elastic.NewBulkUpdateRequest()
			req.UseEasyJSON(true)
			//i2 := docList[i]
			id := m["_id"].(bson.ObjectId)
			//这里需要拿到 object Id 然后再把 m里面的 _id 删掉
			req.Id(id.Hex())
			delete(m, "_id")
			req.Type("_doc")
			req.Index(ns.Str())
			req.Doc(m)
			req.DocAsUpsert(true)
			if _, err := req.Source(); err == nil {
				exec.esbulk.Add(req)
			} else {
				conf.Eslog.Error(err)
				return err
			}
		}
		//exec.esbulk
	}
	return nil
}

// heavy operation. insert data one by one and handle the return error.
func (exec *DocExecutor) tryOneByOne(input []interface{}, index int, collectionHandler *mgo.Collection) error {
	for i := index; i < len(input); i++ {
		raw := input[i]
		var docD bson.D
		if err := bson.Unmarshal(raw.(*bson.Raw).Data, &docD); err != nil {
			return fmt.Errorf("unmarshal data[%v] failed[%v]", raw, err)
		}

		err := collectionHandler.Insert(docD)
		if err == nil {
			continue
		} else if !mgo.IsDup(err) {
			return err
		}

		// only handle duplicate key error
		id := oplog.GetKey(docD, "")

		// orphan document enable and source is sharding
		if conf.Options.FullSyncExecutorFilterOrphanDocument && exec.syncer.orphanFilter != nil {
			// judge whether is orphan document, pass if so
			if exec.syncer.orphanFilter.Filter(docD, collectionHandler.FullName) {
				LOG.Info("orphan document with _id[%v] filter", id)
				continue
			}
		}

		if !conf.Options.FullSyncExecutorInsertOnDupUpdate {
			return fmt.Errorf("duplicate key error[%v], you can clean the document on the target mongodb, "+
				"or enable %v to solve, but full-sync stage needs restart",
				err, "full_sync.executor.insert_on_dup_update")
		} else {
			// convert insert operation to update operation
			if id == nil {
				return fmt.Errorf("parse '_id' from document[%v] failed", docD)
			}
			if err := collectionHandler.UpdateId(id, docD); err != nil {
				return fmt.Errorf("convert oplog[%v] from insert to update run failed[%v]", docD, err)
			}
		}
	}

	// all finish
	return nil
}
