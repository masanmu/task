package index

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	Mdb "github.com/open-falcon/common/db"
	cutils "github.com/open-falcon/common/utils"
	cron "github.com/toolkits/cron"
	ntime "github.com/toolkits/time"

	"github.com/open-falcon/task/g"
	"github.com/open-falcon/task/proc"
)

const (
	indexDeleteCronSpec = "0 0 2 ? * 6" // 每周6晚上22:00执行一次
	deteleStepInSec     = 7 * 24 * 3600 // 索引的最大生存周期, sec
)

var (
	indexDeleteCron = cron.New()
)

// 启动 索引全量更新 定时任务
func StartIndexDeleteTask() {
	indexDeleteCron.AddFuncCC(indexDeleteCronSpec, func() { DeleteIndex() }, 1)
	indexDeleteCron.Start()
}

// 索引的全量更新
func DeleteIndex() {
	startTs := time.Now().Unix()
	deleteIndex()
	endTs := time.Now().Unix()
	log.Printf("deleteIndex, start %s, ts %ds", ntime.FormatTs(startTs), endTs-startTs)

	// statistics
	proc.IndexDeleteCnt.Incr()
}

// 先select 得到可能被删除的index的信息, 然后以相同的条件delete. select和delete不是原子操作,可能有一些不一致,但不影响正确性
func deleteIndex() error {
	dbConn, err := GetDbConn()
	if err != nil {
		log.Println("[ERROR] get dbConn fail", err)
		return err
	}
	defer dbConn.Close()

	ts := time.Now().Unix()
	lastTs := ts - deteleStepInSec
	log.Printf("deleteIndex, lastTs %d\n", lastTs)

	// reinit statistics
	proc.IndexDeleteCnt.PutOther("deleteCntEndpoint", 0)
	proc.IndexDeleteCnt.PutOther("deleteCntTagEndpoint", 0)
	proc.IndexDeleteCnt.PutOther("deleteCntEndpointCounter", 0)

	// endpoint表
	{
		// select
		rows, err := dbConn.Query("SELECT id, endpoint FROM endpoint WHERE ts < ?", lastTs)
		if err != nil {
			log.Println(err)
			return err
		}

		cnt := 0
		for rows.Next() {
			item := &Mdb.GraphEndpoint{}
			err := rows.Scan(&item.Id, &item.Endpoint)
			if err != nil {
				log.Println(err)
				return err
			}
			log.Println("will delete endpoint:", item)
			cnt++
		}

		if err = rows.Err(); err != nil {
			log.Println(err)
			return err
		}

		// delete
		_, err = dbConn.Exec("DELETE FROM endpoint WHERE ts < ?", lastTs)
		if err != nil {
			log.Println(err)
			return err
		}
		log.Printf("delete endpoint, done, cnt %d\n", cnt)

		// statistics
		proc.IndexDeleteCnt.PutOther("deleteCntEndpoint", cnt)
	}

	// tag_endpoint表
	{
		// select
		rows, err := dbConn.Query("SELECT id, tag, endpoint_id FROM tag_endpoint WHERE ts < ?", lastTs)
		if err != nil {
			log.Println(err)
			return err
		}

		cnt := 0
		for rows.Next() {
			item := &Mdb.GraphTagEndpoint{}
			err := rows.Scan(&item.Id, &item.Tag, &item.EndpointId)
			if err != nil {
				log.Println(err)
				return err
			}
			log.Println("will delete tag_endpoint:", item)
			cnt++
		}

		if err = rows.Err(); err != nil {
			log.Println(err)
			return err
		}

		// delete
		_, err = dbConn.Exec("DELETE FROM tag_endpoint WHERE ts < ?", lastTs)
		if err != nil {
			log.Println(err)
			return err
		}
		log.Printf("delete tag_endpoint, done, cnt %d\n", cnt)

		// statistics
		proc.IndexDeleteCnt.PutOther("deleteCntTagEndpoint", cnt)
	}
	// endpoint_counter表
	{
		// select
		rows, err := dbConn.Query("SELECT id, endpoint_id, counter,step,type FROM endpoint_counter WHERE ts < ?", lastTs)
		if err != nil {
			log.Println(err)
			return err
		}
		endpoint_counter := make(map[string][]Mdb.GraphEndpointCounter)
		cnt := 0
		for rows.Next() {
			item := &Mdb.GraphEndpointCounter{}
			err := rows.Scan(&item.Id, &item.EndpointId, &item.Counter, &item.Step, &item.Type)
			if err != nil {
				log.Println(err)
				return err
			}
			var endpoint string
			err = dbConn.QueryRow("SELECT endpoint from endpoint where id = ?", int(item.EndpointId)).Scan(&endpoint)
			if err != nil {
				log.Println(err)
				return err
			}
			endpoint_counter[endpoint] = append(endpoint_counter[endpoint], Mdb.GraphEndpointCounter{Id: item.Id, Step: item.Step, Type: item.Type, Counter: item.Counter, EndpointId: item.EndpointId})
			log.Println("will delete endpoint_counter:", item)
			cnt++
		}

		if err = rows.Err(); err != nil {
			log.Println(err)
			return err
		}
		// delete
		_, err = dbConn.Exec("DELETE FROM endpoint_counter WHERE ts < ?", lastTs)
		if err != nil {
			log.Println(err)
			return err
		}
		log.Printf("delete endpoint_counter, done, cnt %d\n", cnt)
		err = deleteRRd(endpoint_counter)
		// statistics
		proc.IndexDeleteCnt.PutOther("deleteCntEndpointCounter", cnt)
	}

	return nil
}

func deleteRRd(endpoint_counter map[string][]Mdb.GraphEndpointCounter) (err error) {
	cfg := g.Config()
	base := cfg.RRd.Storage
	for endpoint, counters := range endpoint_counter {
		for counter := range counters {
			metric_tag := strings.SplitN(counters[counter].Counter, "/", 2)
			err, tags := cutils.SplitTagsString(metric_tag[len(metric_tag)-1])
			if err != nil {
				log.Println(err)
			}
			checksum := cutils.Checksum(endpoint, metric_tag[0], tags)
			step := fmt.Sprintf("%d", counters[counter].Step)
			rrdfile := base + "/" + checksum[0:2] + "/" + checksum + "_" + counters[counter].Type + "_" + step + ".rrd"
			err = os.Remove(rrdfile)
			if err != nil {
				log.Println("file remove Error")
			} else {
				log.Println("file remove ok")
			}
		}
	}
	return
}
