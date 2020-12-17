package tunnel

import (
	"context"
	"github.com/olivere/elastic"
	LOG "github.com/vinllen/log4go"
	"github.com/vinllen/mgo/bson"
	mogobson "go.mongodb.org/mongo-driver/bson"
	docyncer "mongoshake/collector/docsyncer"
	"mongoshake/tunnel/kafka"
)

type ESWriter struct {
	RemoteAddr string
	client     *elastic.Client
	writer     *kafka.SyncWriter
	bulk       *elastic.BulkProcessor
}

func (tunnel *ESWriter) Name() string {
	return "Elastic Search"
}

func (tunnel *ESWriter) Prepare() bool {
	client, err := docyncer.NewElasticClient(tunnel.RemoteAddr)
	if err != nil {
		LOG.Critical("ESClient prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	processor, err := docyncer.NewBulkProcessor(client)
	if err != nil {
		LOG.Critical("ESClient prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	tunnel.bulk = processor
	tunnel.client = client
	return true
}

func (tunnel *ESWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}
	var m = make(map[string]interface{})
	for index, byteArray := range message.TMessage.RawLogs {
		LOG.Debug(index)
		bson.Unmarshal(byteArray, &m)
		if m["op"] == "u" || m["op"] == "i" {
			doc := getUpdateOrInsertDoc(m)
			if _, err := doc.Source(); err == nil {
				tunnel.bulk.Add(doc)
			} else {
				LOG.Error(err)
				return -1
			}
		} else if m["op"] == "d" {
			id := getOplogObjectId(m)
			delDoc(id.Hex(), m["ns"].(string), tunnel)
		}
		LOG.Debug("byteArray = %v", m)
	}
	for _, log := range message.ParsedLogs {
		object := log.ParsedLog.Object
		jsonDocument, err := mogobson.MarshalExtJSON(object, true, true)
		if err != nil && jsonDocument != nil {
			LOG.Error(err)
		}
		//LOG.Debug("jsondocument = %v", jsonDocument)
		//d := log.ParsedLog.Object.Map()
		//keys := make(map[string]interface{})
		//bytes := []byte(fmt.Sprintf("%v", log.ParsedLog.Object))
		//
		//bson.Unmarshal(bytes, keys)
		//LOG.Debug("Unmarshal: keys= %v", keys)
		//m, txt := oplog.ConvertBsonD2M(log.ParsedLog.Object)
		//
		////log.ParsedLog
		//LOG.Debug("m=%v  aa=%v", m, txt)
		////delete(log.ParsedLog,"")
		//id := log.ParsedLog.Object[0].Value.(bson.ObjectId).Hex()
		//LOG.Debug("d = %v ", d)
		//if log.Operation == "u" || log.Operation == "i" {
		//	req := elastic.NewBulkUpdateRequest()
		//	req.Index()
		//	req.Type("_doc")
		//	req.Id(id)
		//	m, _ := oplog.ConvertBsonD2M(log.ParsedLog.Object)
		//	delete(m, "_id")
		//	req.UseEasyJSON(true)
		//	req.Doc(m)
		//	tunnel.bulk.Add(req)
		//} else if log.Operation == "d" {
		//	delDoc(id, log.Namespace, tunnel)
		//}
		//LOG.Debug("拿到log 数据格式为 %v ", log)

	}
	return ReplyOK
}

func getOplogObjectId(oplog map[string]interface{}) bson.ObjectId {
	m2 := oplog["o"].(map[string]interface{})
	id := m2["_id"].(bson.ObjectId)
	return id
}

func getUpdateOrInsertDoc(oplog map[string]interface{}) *elastic.BulkUpdateRequest {
	req := elastic.NewBulkUpdateRequest()
	i := oplog["ns"].(string)
	m2 := oplog["o"].(map[string]interface{})
	if m2 != nil {
		id := m2["_id"].(bson.ObjectId)
		delete(m2, "_id")
		req.Index(i)
		req.Type("_doc")
		req.Id(id.Hex())
		req.DocAsUpsert(true)
		req.UseEasyJSON(true)
		req.Doc(m2)
	}
	return req
}
func delDoc(id string, ns string, tunnel *ESWriter) {
	req := elastic.NewBulkDeleteRequest()
	search := tunnel.client.Search()
	termQuery := elastic.NewTermQuery("_id", id)
	search.Query(termQuery)
	req.Index(ns)
	req.Type("_doc")
	req.Id(id)
	req.UseEasyJSON(true)
	searchResult, err := search.Do(context.Background())
	if err != nil {
		LOG.Debug("Unable to delete document %s: %s",
			id, err)
	}
	if searchResult.Hits != nil && searchResult.Hits.TotalHits == 1 {
		hit := searchResult.Hits.Hits[0]
		req.Index(hit.Index)
		req.Type(hit.Type)
		if hit.Routing != "" {
			req.Routing(hit.Routing)
		}
		if hit.Parent != "" {
			req.Parent(hit.Parent)
		}
	} else {
		LOG.Debug("Failed to find unique document %s", id)
	}
}

func (tunnel *ESWriter) AckRequired() bool {
	return false
}

func (tunnel *ESWriter) ParsedLogsRequired() bool {
	return false
}
