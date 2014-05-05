package library

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/nytlabs/gojee"
	"github.com/nytlabs/streamtools/st/blocks" // blocks
	"github.com/nytlabs/streamtools/st/util"
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

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func formatReply(reply interface{}) (interface{}, error) {
	switch reply := reply.(type) {
	case []interface{}:
		result := make([]string, len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			p, ok := reply[i].([]byte)
			if !ok {
				return nil, fmt.Errorf("unexpected element type for string, got type %T", reply[i])
			}
			result[i] = string(p)
		}
		return result, nil
	case string:
		return reply, nil
	case int:
		result := int(reply)
		return result, nil
	case int64:
		result := int(reply)
		return result, nil
	case []byte:
		return string(reply), nil
	}
	return nil, fmt.Errorf("some other error happened")
}

// Setup is called once before running the block. We build up the channels and specify what kind of block this is.
func (b *Redis) Setup() {
	b.Kind = "Redis"
	b.Desc = "sends arbitrary commands to redis"
	b.in = b.InRoute("in")
	b.inrule = b.InRoute("rule")
	b.queryrule = b.QueryRoute("rule")
	b.quit = b.Quit()
	b.out = b.Broadcast()
}

// Run is the block's main loop. Here we listen on the different channels we set up.
func (b *Redis) Run() {
	var server string
	var password string
	var command string
	var arguments []string
	var argumentTrees []*jee.TokenTree
	var err error
	var pool *redis.Pool

	for {
		select {
		case ruleI := <-b.inrule:
			server, err = util.ParseString(ruleI, "Server")
			if err != nil {
				b.Error(err)
				continue
			}
			password, err = util.ParseString(ruleI, "Password")
			if err != nil {
				b.Error(err)
				continue
			}
			command, err = util.ParseString(ruleI, "Command")
			if err != nil {
				b.Error(err)
				continue
			}

			arguments, err = util.ParseArrayString(ruleI, "Arguments")
			if err != nil {
				b.Error(err)
				continue
			}

			if len(arguments) > 0 {
				argumentTrees = make([]*jee.TokenTree, len(arguments))
				for i, path := range arguments {
					token, err := jee.Lexer(path)
					if err != nil {
						b.Error(err)
						continue
					}
					tree, err := jee.Parser(token)
					if err != nil {
						b.Error(err)
						continue
					}
					argumentTrees[i] = tree
				}
			}
			pool = newPool(server, password)

		case responseChan := <-b.queryrule:
			// deal with a query request
			responseChan <- map[string]interface{}{
				"Server":    server,
				"Password":  password,
				"Command":   command,
				"Arguments": arguments,
			}
		case <-b.quit:
			// quit the block
			return
		case msg := <-b.in:
			conn := pool.Get()
			defer conn.Close()

			args := make([]interface{}, len(argumentTrees))
			for i, tree := range argumentTrees {
				argument, err := jee.Eval(tree, msg)
				if err != nil {
					b.Error(err)
					break
				}
				args[i] = argument
			}

			// commands like 'KEYS *' or 'SET NUMBERS 1'
			n, err := conn.Do(command, args...)
			if err != nil {
				b.Error(err)
				break
			}

			formatted, err := formatReply(n)
			if err != nil {
				b.Error(err)
				break
			}

			out := map[string]interface{}{
				"response": formatted,
			}
			b.out <- out
		}
	}
}
