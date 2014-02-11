package newblocks

import "log"

type MsgData map[string]interface{}

type Msg struct {
	msg   MsgData
	route string
}

type Block struct {
	Name    string
	Rule    map[string]interface{}
	InChan  chan Msg
	OutChan chan Msg
	ErrChan chan error
}

type BlockInterface interface {
	GetName() string
	GetInChan() chan Msg
	GetOutChan() chan Msg
	GetErrChan() chan error
	GetRule(chan map[string]interface{}) error
	SetRule(MsgData) error
	Setup() error
}

func (b *Block) GetName() string {
	return b.Name
}

func (b *Block) GetInChan() chan Msg {
	return b.InChan
}

func (b *Block) GetOutChan() chan Msg {
	return b.OutChan
}

func (b *Block) GetErrChan() chan error {
	return b.ErrChan
}

func (b *Block) Setup() error {
	log.Println("setting up default rule")
	b.Rule = map[string]interface{}{}
	return nil
}

func (b *Block) GetRule(respChan chan map[string]interface{}) error {
	log.Println("getting rule")
	respChan <- b.Rule
	return nil
}

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

func (b *Block) AddHandler(route string, f interface{}) {
}

// possible behaviours

type Transform interface {
	TransformMessage(MsgData) error
}

type Sink interface {
	WriteExternalMessage(MsgData) error
}

type State interface {
	ModifyState()
	PollState()
	QueryState()
}

type Generator interface {
	EmitMessage()
}

type Source interface {
	ReadExternalMessage()
}

// the main block routine

func BlockRoutine(b BlockInterface) {

	var err error

	inChan := b.GetInChan()
	errChan := b.GetErrChan()

	b.Setup()

	// see what the block can do
	transformer, doesTransform := interface{}(b).(Transform)
	sinker, doesSink := interface{}(b).(Sink)

	for {
		select {
		case msg := <-inChan:
			if doesTransform {
				err = transformer.TransformMessage(msg.msg)
				if err != nil {
					errChan <- err
				}
			}
			if doesSink {
				err := sinker.WriteExternalMessage(msg.msg)
				if err != nil {
					errChan <- err
				}
			}
		}

	}
}
