package blocks

import (
	"log"
	"testing"
	"time"
)

func TestTicker(t *testing.T) {
	inchan := make(chan Msg)
	outchan := make(chan Msg)
	errchan := make(chan error)
	quitchan := make(chan bool)
	exitchan := make(chan bool)
	chans := BlockChans{
		InChan:   inchan,
		OutChan:  outchan,
		ErrChan:  errchan,
		QuitChan: quitchan,
		ExitChan: exitchan,
	}
	base := Block{
		Name:  "testingBlock",
		Chans: chans,
	}
	b := &Ticker{
		Block: base,
	}
	go BlockRoutine(b)
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		quitchan <- true
	})
	for {
		select {
		case err := <-errchan:
			log.Println(err.Error())
		case msg := <-outchan:
			log.Println(msg)
		case <-exitchan:
			return
		}
	}

}
