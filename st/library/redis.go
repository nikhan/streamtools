package library

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/nytlabs/gojee"                 // jee
	"github.com/nytlabs/streamtools/st/blocks" // blocks
	"github.com/nytlabs/streamtools/st/util"   // util
)

// specify those channels we're going to use to communicate with streamtools
type Redis struct {
	blocks.Block
	queryrule chan blocks.MsgChan
	inrule    blocks.MsgChan
	in        blocks.MsgChan
	out       blocks.MsgChan
	quit      blocks.MsgChan
}

// we need to build a simple factory so that streamtools can make new blocks of this kind
func NewRedis() blocks.BlockInterface {
	return &Redis{}
}

// Setup is called once before running the block. We build up the channels and specify what kind of block this is.
func (b *Redis) Setup() {
	b.Kind = "Redis"
	b.Desc = "executes redis commands with arbitrary number of arguments"
	b.in = b.InRoute("in")
	b.inrule = b.InRoute("rule")
	b.queryrule = b.QueryRoute("rule")
	b.quit = b.Quit()
	b.out = b.Broadcast()
}

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// Run is the block's main loop. Here we listen on the different channels we set up.
func (b *Redis) Run() {
	var command string
	var args []string

	for {
		select {
		case ruleI := <-b.inrule:
			// set a parameter of the block
			command, err = util.ParseString(ruleI, "Command")
			if err != nil {
				b.Error(err)
				break
			}
			args, err = util.ParseArrayString(ruleI, "Arguments")
			if err != nil {
				b.Error(err)
				break
			}
		case <-b.quit:
			// quit the block
			return
			// deal with inbound data
		case msg := <-b.in:
			if tree == nil {
				continue
			}
			v, err := jee.Eval(tree, msg)
			if err != nil {
				b.Error(err)
				break
			}

			if _, ok := v.(string); !ok {
				b.Error(errors.New("can only redis sets of strings"))
				continue
			}

			_, ok := set[v]
			// emit the incoming message if it isn't found in the set
			if !ok {
				b.out <- msg
				set[v] = true // and add it to the set
			}
		case c := <-b.queryrule:
			// deal with a query request
			c <- map[string]interface{}{
				"Path": path,
			}

		}
	}
}
