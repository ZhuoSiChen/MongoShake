package docsyncer

import (
	"fmt"
	nimo "github.com/gugemichael/nimo4go"
	"github.com/olivere/elastic"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	"log"
	conf "mongoshake/collector/configure"
	"mongoshake/collector/filter"
	"mongoshake/collector/transform"
	utils "mongoshake/common"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var traceLog = log.New(os.Stdout, "TRACE ", log.Flags())

type ESSyncer struct {
	// syncer id
	id int
	// source mongodb url
	FromMongoUrl string
	fromReplset  string
	// destination mongodb url
	ToESUrl string
	// index of namespace
	indexMap map[utils.NS][]mgo.Index
	// start time of sync
	startTime time.Time
	// source is sharding?
	FromIsSharding bool

	nsTrans *transform.NamespaceTransform
	// filter orphan duplicate record
	orphanFilter *filter.OrphanFilter

	mutex sync.Mutex

	qos *utils.Qos // not owned

	replMetric *utils.ReplicationMetric

	client *elastic.Client
	// below are metric info
	metricNsMapLock sync.Mutex
	metricNsMap     map[utils.NS]*CollectionMetric // namespace map: db.collection -> collection metric
}

func NewESSyncer(id int, fromMongoUrl string, fromReplset string, toMongoUrl string, nsTrans *transform.NamespaceTransform, orphanFilter *filter.OrphanFilter, qos *utils.Qos, fromIsSharding bool) *ESSyncer {

	client, err := newElasticClient(toMongoUrl)
	if err != nil {
		return nil
	}
	esSyncer := &ESSyncer{
		id:             id,
		FromMongoUrl:   fromMongoUrl,
		fromReplset:    fromReplset,
		ToESUrl:        toMongoUrl,
		nsTrans:        nsTrans,
		orphanFilter:   orphanFilter,
		qos:            qos,
		metricNsMap:    make(map[utils.NS]*CollectionMetric),
		replMetric:     utils.NewMetric(fromReplset, utils.TypeFull, utils.METRIC_TPS),
		FromIsSharding: fromIsSharding,
		client:         client,
	}
	return esSyncer
}

func (esSyncer *ESSyncer) Init() {

}

func (syncer *ESSyncer) Start() (syncError error) {
	syncer.startTime = time.Now()
	var wg sync.WaitGroup

	// get all namespace
	nsList, _, err := GetDbNamespace(syncer.FromMongoUrl)
	if err != nil {
		return err
	}

	if len(nsList) == 0 {
		LOG.Info("%s finish, but no data", syncer)
		return
	}

	// create metric for each collection
	for _, ns := range nsList {
		syncer.metricNsMap[ns] = NewCollectionMetric()
	}

	collExecutorParallel := conf.Options.FullSyncReaderCollectionParallel
	namespaces := make(chan utils.NS, collExecutorParallel)

	wg.Add(len(nsList))

	nimo.GoRoutine(func() {
		for _, ns := range nsList {
			namespaces <- ns
		}
	})

	// run collection sync in parallel
	var nsDoneCount int32 = 0
	for i := 0; i < collExecutorParallel; i++ {
		collExecutorId := GenerateCollExecutorId()
		nimo.GoRoutine(func() {
			for {
				ns, ok := <-namespaces
				if !ok {
					break
				}

				toNS := utils.NewNS(syncer.nsTrans.Transform(ns.Str()))

				LOG.Info("%s ES collExecutor-%d sync ns %v to %v begin", syncer, collExecutorId, ns, toNS)
				err := syncer.collectionSync(collExecutorId, ns, toNS)
				atomic.AddInt32(&nsDoneCount, 1)

				if err != nil {
					LOG.Critical("%s collExecutor-%d sync ns %v to %v failed. %v",
						syncer, collExecutorId, ns, toNS, err)
					syncError = fmt.Errorf("document syncer sync ns %v to %v failed. %v", ns, toNS, err)
				} else {
					process := int(atomic.LoadInt32(&nsDoneCount)) * 100 / len(nsList)
					LOG.Info("%s collExecutor-%d sync ns %v to %v successful. db syncer-%d progress %v%%",
						syncer, collExecutorId, ns, toNS, syncer.id, process)
				}
				wg.Done()
			}
			LOG.Info("%s collExecutor-%d finish", syncer, collExecutorId)
		})
	}

	wg.Wait()
	close(namespaces)

	return syncError
}

func (syncer *ESSyncer) collectionSync(collExecutorId int, ns utils.NS, toNS utils.NS) error {
	// writer
	colExecutor := NewCollectionExecutorToES(collExecutorId, syncer.ToESUrl, toNS, syncer)
	if err := colExecutor.StartForEs(); err != nil {
		return err
	}

	// splitter reader
	//在这里读取数据库 collection 与document
	splitter := NewDocumentSplitter(syncer.FromMongoUrl, ns)
	if splitter == nil {
		return fmt.Errorf("create splitter failed")
	}
	defer splitter.Close()

	// metric
	collectionMetric := syncer.metricNsMap[ns]
	collectionMetric.CollectionStatus = StatusProcessing
	collectionMetric.TotalCount = splitter.count

	// run in several pieces
	var wg sync.WaitGroup
	wg.Add(splitter.pieceNumber)
	for i := 0; i < SpliterReader; i++ {
		go func() {
			for {
				reader, ok := <-splitter.readerChan
				if !ok {
					break
				}

				if err := syncer.splitSync(reader, colExecutor, collectionMetric); err != nil {
					LOG.Crashf("%v", err)
				}

				wg.Done()
			}
		}()
	}
	wg.Wait()

	// close writer
	if err := colExecutor.Wait(); err != nil {
		return fmt.Errorf("close writer failed: %v", err)
	}

	/*
	 * in the former version, we fetch indexes after all data finished. However, it'll
	 * have problem if the index is build/delete/update in the full-sync stage, the oplog
	 * will be replayed again, e.g., build index, which must be wrong.
	 */
	// fetch index

	// set collection finish
	collectionMetric.CollectionStatus = StatusFinish
	return nil
}

func (esSyncer *ESSyncer) Close() {

}

type docWithNameSpace struct {
	doc bson.Raw
	ns  string
}

func (esSyncer *ESSyncer) splitSync(reader *DocumentReader, colExecutor *CollectionExecutor, collectionMetric *CollectionMetric) error {

	bufferSize := conf.Options.FullSyncReaderDocumentBatchSize
	buffer := make([]*bson.Raw, 0, bufferSize)
	bufferByteSize := 0

	for {
		doc, err := reader.NextDoc()
		if err != nil {
			return fmt.Errorf("splitter reader[%v] get next document failed: %v", reader, err)
		} else if doc == nil {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			break
		}

		esSyncer.replMetric.AddGet(1)
		esSyncer.replMetric.AddSuccess(1) // only used to calculate the tps which is extract from "success"

		if bufferByteSize+len(doc.Data) > MAX_BUFFER_BYTE_SIZE || len(buffer) >= bufferSize {
			atomic.AddUint64(&collectionMetric.FinishCount, uint64(len(buffer)))
			colExecutor.Sync(buffer)
			buffer = make([]*bson.Raw, 0, bufferSize)
			bufferByteSize = 0
		}

		// transform dbref for document
		if len(conf.Options.TransformNamespace) > 0 && conf.Options.IncrSyncDBRef {
			var docData bson.D
			if err := bson.Unmarshal(doc.Data, docData); err != nil {
				LOG.Error("splitter reader[%v] do bson unmarshal %v failed. %v", reader, doc.Data, err)
			} else {
				docData = transform.TransformDBRef(docData, reader.ns.Database, esSyncer.nsTrans)
				if v, err := bson.Marshal(docData); err != nil {
					LOG.Warn("splitter reader[%v] do bson marshal %v failed. %v", reader, docData, err)
				} else {
					doc.Data = v
				}
			}
		}
		buffer = append(buffer, doc)
		bufferByteSize += len(doc.Data)
	}

	LOG.Info("splitter reader finishes: %v", reader)
	reader.Close()
	return nil
}

func StartDropDestIndex() {

}

func newElasticClient(esUrl string) (client *elastic.Client, err error) {

	var clientOptions []elastic.ClientOptionFunc
	transport := &http.Transport{
		//DisableCompression:  !config.Gzip,
		TLSHandshakeTimeout: time.Duration(30) * time.Second,
		//TLSClientConfig:     tlsConfig,
	}

	httpClient := &http.Client{
		//Timeout:   time.Duration(config.ElasticClientTimeout) * time.Second,
		Transport: transport,
	}

	clientOptions = append(clientOptions, elastic.SetTraceLog(traceLog))
	clientOptions = append(clientOptions, elastic.SetHttpClient(httpClient))
	clientOptions = append(clientOptions, elastic.SetURL(esUrl))

	return elastic.NewClient(clientOptions...)
}
