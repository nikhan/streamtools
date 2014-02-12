package blocks

import "log"

type MsgData map[string]interface{}

type Msg struct {
	// msg is the payload map string interface
	msg MsgData
	// route informs the BlockRoutine on how to handle the message
	route string
}

type BlockChans struct {
	// inbound messages come through the InChan. Their route string governs how
	// they will be processed by the block
	InChan chan Msg
	// TODO outbound messages will be broadcast to all listening blocks. This OutChan
	// is a placeholder till we implement the broadcast
	OutChan chan Msg
	// any errors generated get sent out on the err channel to be handled by
	// whatever set the BlockRoutine going
	ErrChan chan error
	// the quit channel carries simple signals to tell the block to quit
	QuitChan chan bool
	// TODO the block will broadcast an exit event to all blocks connected to its
	// InChan. Similar mechanics need to be used as for the outbound broadcast
	ExitChan chan bool
}

type Block struct {
	Name  string // the name of the block specifed by the user (like MyBlock)
	Kind  string // the kind of block this is (like count, toFile, fromSQS)
	Rule  map[string]interface{}
	Chans BlockChans
}

// A Block must satisfy, at the very minium, this interface. All blocks that
// inherit from Block satisfies these by default, though any of them can be
// overriden.
type BlockInterface interface {
	getName() string
	getKind() string
	getChans() BlockChans
	getRule(chan map[string]interface{}) error
	setRule(MsgData) error
	setup() error
	tidyUp() error
}

// returns the name of the block
func (b *Block) getName() string {
	return b.Name
}

// returns the kind of the block
func (b *Block) getKind() string {
	return b.Kind
}

// returns the block's component channels
func (b *Block) getChans() BlockChans {
	return b.Chans
}

// Setup is called before the block starts listening for messages of any kind
func (b *Block) setup() error {
	log.Println("setting up default rule")
	b.Rule = map[string]interface{}{}
	return nil
}

// TidyUp is to close anything that needs closing before the block quits
func (b *Block) tidyUp() error {
	return nil
}

// GetRule returns the block's rule
func (b *Block) getRule(respChan chan map[string]interface{}) error {
	log.Println("getting rule")
	respChan <- b.Rule
	return nil
}

// SetRule specifies the block's rule
func (b *Block) setRule(msg MsgData) error {
	log.Println("setting rule")
	for key := range msg {
		log.Println(key)
		rule, ok := msg[key]
		if !ok {
			log.Println(key, "rule was not specified")
			continue
		}
		_, ok = b.Rule[key]
		if !ok {
			log.Println("this block does not have a", key, "rule")
			continue
		}
		b.Rule[key] = rule
	}
	return nil
}

// TODO this method is currently unused, but could be used in the future to set
// custom handlers
func (b *Block) AddHandler(route string, f interface{}) {
}

// possible behaviours

type Transform interface {
	TransformMessage(MsgData) (MsgData, error)
}

type Sink interface {
	WriteExternalMessage(MsgData) error
}

type State interface {
	ModifyState() error
	PollState() (MsgData, error)
	QueryState() error
}

type Generator interface {
	EmitMessages(chan MsgData)
}

type Source interface {
	ReadExternalMessage() (MsgData, error)
}

type RuleBound interface {
	setRule(MsgData) error
	getRule(chan map[string]interface{}) error
}

// the main block routine

func BlockRoutine(b BlockInterface) {

	c := b.getChans()

	b.setup()

	// see what the block can do
	transformer, doesTransform := interface{}(b).(Transform)
	sinker, doesSink := interface{}(b).(Sink)
	ruler, doesRule := interface{}(b).(RuleBound)
	generator, doesGenerate := interface{}(b).(Generator)

	genChan := make(chan MsgData)
	if doesGenerate {
		go func() {
			generator.EmitMessages(genChan)
		}()
	}

	for {
		select {
		case msg := <-c.InChan:
			log.Println("inbound route:", msg.route)
			switch msg.route {
			case "in":
				if doesTransform {
					outMsg, err := transformer.TransformMessage(msg.msg)
					if err != nil {
						c.ErrChan <- err
					}
					if outMsg != nil {
						c.OutChan <- Msg{
							msg:   outMsg,
							route: "in",
						}
					}
				}
				if doesSink {
					err := sinker.WriteExternalMessage(msg.msg)
					if err != nil {
						c.ErrChan <- err
					}
				}
			case "setrule":
				if doesRule {
					ruler.setRule(msg.msg)
				}
			case "":
				// connections inbound

			}
		case msg := <-genChan:
			c.OutChan <- Msg{
				msg: msg,
			}
		case <-c.QuitChan:
			log.Println("tidying")
			b.tidyUp()
			log.Println("quitting", b.getKind(), b.getName())
			c.ExitChan <- true
			return
		}
	}
}
