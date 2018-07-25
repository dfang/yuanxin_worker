package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jpfuentes2/go-env/autoload"

	"github.com/gocraft/work"
	"github.com/gomodule/redigo/redis"

	im "github.com/dfang/netease-im"
	"github.com/dfang/yuanxin_api/model"
	// "github.com/dfang/yuanxin_api"
)

var redisConnString = fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT"))

// Make a redis pool
var redisPool = &redis.Pool{
	MaxActive: 5,
	MaxIdle:   5,
	Wait:      true,
	Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", redisConnString)
	},
}
var conn = &sql.DB{}

// var client = im.Init("d45545b3eeb821970eab26931859871e", "d31182026a36")
var client = im.Init(os.Getenv("NETEASE_IM_APPID"), os.Getenv("NETEASE_IM_APPKEY"))

type Context struct {
	userID int
	// client *im.NeteaseIM
	// conn   *sql.DB
}

func main() {

	// context := Context{
	// 	client: im.Init("d45545b3eeb821970eab26931859871e", "d31182026a36"),
	// 	conn:   conn,
	// }
	// Make a new pool. Arguments:
	// Context{} is a struct that will be the context for the request.
	// 10 is the max concurrency
	// "work" is the Redis namespace
	// redisPool is a Redis pool
	pool := work.NewWorkerPool(Context{}, 10, "work", redisPool)

	// Add middleware that will be executed for each job
	pool.Middleware((*Context).Log)
	pool.Middleware((*Context).FindUser)

	// Map the name of jobs to handler functions
	pool.Job("register_user_to_netease_im", (*Context).RegisterAccid)

	// Customize options:
	// pool.JobWithOptions("export", work.JobOptions{Priority: 10, MaxFails: 1}, (*Context).Export)

	// Start processing jobs
	pool.Start()

	// Wait for a signal to quit:
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, os.Kill)
	<-signalChan

	// Stop the pool
	pool.Stop()
}

// Log
func (c *Context) Log(job *work.Job, next work.NextMiddlewareFunc) error {
	fmt.Println("Starting job: ", job.Name)
	return next()
}

func (c *Context) FindUser(job *work.Job, next work.NextMiddlewareFunc) error {
	// If there's a customer_id param, set it in the context for future middleware and handlers to use.
	if _, ok := job.Args["user_id"]; ok {
		c.userID = int(job.ArgInt64("user_id"))
		if err := job.ArgError(); err != nil {
			return err
		}
	}
	return next()
}

func (c *Context) RegisterAccid(job *work.Job) error {
	// Extract arguments:
	// accid := job.ArgString("accid")
	// token := job.ArgString("token")
	// if err := job.ArgError(); err != nil {
	// 	return err
	// }
	// log.Printf("accid %s \n", accid)
	// log.Printf("token %s \n", token)

	// log.Println("hello")
	// log.Println(c.conn)
	// log.Println(c.client)

	connectionString := fmt.Sprintf("%s:%s@%s/%s?parseTime=true", os.Getenv("APP_DB_USER"), os.Getenv("APP_DB_PASSWORD"), os.Getenv("APP_DB_HOST"), os.Getenv("APP_DB_NAME"))
	// connectionString := fmt.Sprintf("%s:%s@%s/%s?parseTime=true", "root", "", "tcp(127.0.0.1:3306)", "news")
	conn, err := sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Ping()
	if err != nil {
		log.Fatal(err)
	}

	user, err := model.UserByID(conn, c.userID)
	if err != nil || user == nil {
		log.Printf("找不到user_id为%d的用户\n", c.userID)
	}
	if user == nil {
		log.Println("user is nil")
	}

	// Register to netease.im
	info := im.UserInfo{
		Accid: genAccid(user.Email.String, user.Phone.String),
	}
	result := client.CreateAccid(info)

	// Update to db
	err = user.UpdateAccInfo(conn)
	if err == nil {
		log.Println(result)
		fmt.Println("job is done !")
	}

	return nil
}

func (c *Context) Export(job *work.Job) error {
	return nil
}

func (c *Context) CrawNews(job *work.Job) error {
	return nil
}

func genAccid(email, phone string) string {
	h := md5.New()
	io.WriteString(h, email)
	io.WriteString(h, phone)
	return hex.EncodeToString(h.Sum(nil))
}
