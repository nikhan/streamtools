package library

import (
	"errors"
	"log"
	"time"

	"github.com/fzzy/radix/redis"
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
	var iHateThis string
	var command string
	var arguments []string
	var argumentTrees []*jee.TokenTree
	var err error

	for {
		select {
		case ruleI := <-b.inrule:
			server, err = util.ParseString(ruleI, "Server")
			if err != nil {
				b.Error(err)
				continue
			}
			iHateThis = server
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

		case responseChan := <-b.queryrule:
			log.Println(iHateThis)
			// deal with a query request
			responseChan <- map[string]interface{}{
				"Server":    server,
				"Command":   command,
				"Arguments": arguments,
			}
		case <-b.quit:
			// quit the block
			return
		case msg := <-b.in:
			log.Println("server:", server)
			log.Println("iHateThis:", iHateThis)
			client, err := redis.DialTimeout("tcp", iHateThis, time.Duration(10)*time.Second)
			if err != nil {
				b.Error(err)
				continue
			}
			defer client.Close()

			var reply *redis.Reply

			if argumentTrees != nil {
				x := make([]string, len(argumentTrees))
				for i, tree := range argumentTrees {
					argument, err := jee.Eval(tree, msg)
					if err != nil {
						b.Error(err)
						break
					}
					argS, ok := argument.(string)
					if !ok {
						b.Error(errors.New("arguments must be strings"))
						break
					}
					x[i] = argS
				}

				// commands like 'KEYS *' or 'SET NUMBERS 1'
				reply = client.Cmd(command, x)
			} else {
				// argument-less commands like DBSIZE
				reply = client.Cmd(command)
			}

			if reply.Err != nil {
				b.Error(reply.Err)
				continue
			}

			if reply.Type == redis.ErrorReply {
				b.Error(reply.Err)
				continue

			} else if reply.Type == redis.IntegerReply {
				parsedVar, err := reply.Int()
				if err != nil {
					b.Error(err)
					continue
				}
				out := map[string]interface{}{
					"data": parsedVar,
				}
				b.out <- out
			} else if reply.Type == redis.MultiReply {
				if len(reply.Elems)%2 != 0 {
					parsedVar, err := reply.List()
					if err != nil {
						b.Error(err)
						continue
					}
					out := map[string]interface{}{
						"data": parsedVar,
					}
					b.out <- out
				} else {
					parsedVar, err := reply.Hash()
					if err != nil {
						b.Error(err)
						continue
					}
					out := map[string]interface{}{
						"data": parsedVar,
					}
					b.out <- out
				}
			} else if reply.Type != redis.MultiReply {
				parsedVar, err := reply.Str()
				if err != nil {
					b.Error(err)
					continue
				}
				out := map[string]interface{}{
					"data": parsedVar,
				}
				b.out <- out
			}
		}
	}
}
