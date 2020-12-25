package tunnel

import (
	"context"
	"github.com/olivere/elastic"
	"github.com/vinllen/mgo"
	"github.com/vinllen/mgo/bson"
	mogobson "go.mongodb.org/mongo-driver/bson"
	conf "mongoshake/collector/configure"
	docyncer "mongoshake/collector/docsyncer"
	utils "mongoshake/common"
	"strings"
)

type ESWriter struct {
	RemoteAddr string
	client     *elastic.Client
	bulk       *elastic.BulkProcessor
	db         *mgo.Database
}

func (tunnel *ESWriter) Name() string {
	return "Elastic Search"
}

func (tunnel *ESWriter) Prepare() bool {
	conf.Eslog.Info("aaaaaaaaaaaaaaaaaaaaa")

	client, err := docyncer.NewElasticClient(tunnel.RemoteAddr)
	if err != nil {
		conf.Eslog.Critical("ESClient prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}

	processor, err := docyncer.NewBulkProcessor(client)
	if err != nil {
		conf.Eslog.Critical("ESClient prepare[%v] create writer error[%v]", tunnel.RemoteAddr, err)
		return false
	}
	conn, err := utils.NewMongoConn(conf.GetSafeOptions().MongoUrls[0], conf.Options.MongoConnectMode, true,
		utils.ReadWriteConcernDefault, utils.ReadWriteConcernDefault)
	s := conf.GetSafeOptions().FilterNamespaceWhite[0]
	split := strings.Split(s, ".")
	tunnel.db = conn.Session.DB(split[0])
	tunnel.bulk = processor
	tunnel.client = client
	return true
}

func (tunnel *ESWriter) Send(message *WMessage) int64 {
	if len(message.RawLogs) == 0 || message.Tag&MsgProbe != 0 {
		return 0
	}
	var m = make(map[string]interface{})
	for _, byteArray := range message.TMessage.RawLogs {
		bson.Unmarshal(byteArray, &m)
		if m["op"] == "u" || m["op"] == "i" {
			doc := tunnel.getUpdateOrInsertDoc(m)
			if doc != nil {
				if _, err := doc.Source(); err == nil {
					tunnel.bulk.Add(doc)
				} else {
					conf.Eslog.Error(err)
					return -1
				}
			} else {
				conf.Eslog.Info("met some update o == nil")
			}
		} else if m["op"] == "d" {
			id := getOplogObjectId(m)
			delDoc(id.Hex(), m["ns"].(string), tunnel)
		}
		conf.Eslog.Debug("byteArray = %v", m)
	}
	for _, log := range message.ParsedLogs {
		object := log.ParsedLog.Object
		jsonDocument, err := mogobson.MarshalExtJSON(object, true, true)
		if err != nil && jsonDocument != nil {
			conf.Eslog.Error(err)
		}
		//conf.Eslog.Debug("jsondocument = %v", jsonDocument)
		//d := log.ParsedLog.Object.Map()
		//keys := make(map[string]interface{})
		//bytes := []byte(fmt.Sprintf("%v", log.ParsedLog.Object))
		//
		//bson.Unmarshal(bytes, keys)
		//conf.Eslog.Debug("Unmarshal: keys= %v", keys)
		//m, txt := oplog.ConvertBsonD2M(log.ParsedLog.Object)
		//
		////log.ParsedLog
		//conf.Eslog.Debug("m=%v  aa=%v", m, txt)
		////delete(log.ParsedLog,"")
		//id := log.ParsedLog.Object[0].Value.(bson.ObjectId).Hex()
		//conf.Eslog.Debug("d = %v ", d)
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
		//conf.Eslog.Debug("拿到log 数据格式为 %v ", log)

	}
	return ReplyOK
}

func getOplogObjectId(oplog map[string]interface{}) bson.ObjectId {
	m2 := oplog["o"].(map[string]interface{})
	id := m2["_id"].(bson.ObjectId)
	return id
}

func (tunnel *ESWriter) getUpdateOrInsertDoc(oplog map[string]interface{}) *elastic.BulkUpdateRequest {
	req := elastic.NewBulkUpdateRequest()
	ns := oplog["ns"].(string)
	o := oplog["o"].(map[string]interface{})
	id, ok := o["_id"].(bson.ObjectId)
	if ok {
		delete(o, "_id")
		req.Index(ns)
		req.Type("_doc")
		req.Id(id.Hex())
		req.DocAsUpsert(true)
		req.UseEasyJSON(true)
		req.Doc(o)
		return req
	} else {
		ns := oplog["ns"].(string)
		o2 := oplog["o2"].(map[string]interface{})
		objectId := o2["_id"].(bson.ObjectId)
		split := strings.Split(ns, ".")
		var m map[string]interface{}
		err := tunnel.db.C(split[1]).FindId(objectId).One(&m)
		if err == nil {
			delete(m, "_id")
			req.Index(ns)
			req.Type("_doc")
			req.Id(objectId.Hex())
			req.DocAsUpsert(true)
			req.UseEasyJSON(true)
			req.Doc(m)
			return req
		}

	}
	return nil
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
		conf.Eslog.Error("Unable to delete document %s: %s",
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
		conf.Eslog.Error("Failed to find unique document %s", id)
	}
}

func (tunnel *ESWriter) AckRequired() bool {
	return false
}

func (tunnel *ESWriter) ParsedLogsRequired() bool {
	return false
}
