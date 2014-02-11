package blocks

import "log"

type MsgData map[string]interface{}

type Msg struct {
	// msg is the payload map string interface
	msg MsgData
	// route informs the BlockRoutine on how to handle the message
	route string
}

type Block struct {
	Name string
	Rule map[string]interface{}
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

// A Block must satisfy, at the very minium, this interface. All blocks that
// inherit from Block satisfies these by default, though any of them can be
// overriden.
type BlockInterface interface {
	GetName() string
	GetChans() (chan Msg, chan Msg, chan error, chan bool, chan bool)
	GetRule(chan map[string]interface{}) error
	SetRule(MsgData) error
	Setup() error
}

// returns the name of the block
func (b *Block) GetName() string {
	return b.Name
}

// returns the block's component channels
func (b *Block) GetChans() (chan Msg, chan Msg, chan error, chan bool, chan bool) {
	return b.InChan, b.OutChan, b.ErrChan, b.QuitChan, b.ExitChan
}

// Setup is called before the block starts listening for messages of any kind
func (b *Block) Setup() error {
	log.Println("setting up default rule")
	b.Rule = map[string]interface{}{}
	return nil
}

// GetRule returns the block's rule
func (b *Block) GetRule(respChan chan map[string]interface{}) error {
	log.Println("getting rule")
	respChan <- b.Rule
	return nil
}

// SetRule specifies the block's rule
func (b *Block) SetRule(msg MsgData) error {
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
	EmitMessage() (MsgData, error)
}

type Source interface {
	ReadExternalMessage() (MsgData, error)
}

type RuleBound interface {
	SetRule(MsgData) error
	GetRule(chan map[string]interface{}) error
}

// the main block routine

func BlockRoutine(b BlockInterface) {

	inChan, outChan, errChan, quitChan, exitChan := b.GetChans()

	b.Setup()

	// see what the block can do
	transformer, doesTransform := interface{}(b).(Transform)
	sinker, doesSink := interface{}(b).(Sink)
	ruler, doesRule := interface{}(b).(RuleBound)

	for {
		select {
		case msg := <-inChan:
			log.Println(msg.route)
			switch msg.route {
			case "in":
				if doesTransform {
					outMsg, err := transformer.TransformMessage(msg.msg)
					if err != nil {
						errChan <- err
					}
					if outMsg != nil {
						outChan <- Msg{
							msg:   outMsg,
							route: "in",
						}
					}
				}
				if doesSink {
					err := sinker.WriteExternalMessage(msg.msg)
					if err != nil {
						errChan <- err
					}
				}
			case "setrule":
				if doesRule {
					ruler.SetRule(msg.msg)
				}
			}
		case <-quitChan:
			log.Println("quitting")
			exitChan <- true
			return
		}

	}
}
