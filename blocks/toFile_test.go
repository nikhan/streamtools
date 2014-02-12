package blocks

import (
	"log"
	"testing"
	"time"
)

func TestToFile(t *testing.T) {
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
	b := &ToFile{
		Block: base,
	}
	setrulemsg := Msg{
		msg:   map[string]interface{}{"Filename": "test.out"},
		route: "setrule",
	}
	go BlockRoutine(b)
	inchan <- setrulemsg
	writemsg := Msg{
		msg:   map[string]interface{}{"test": true},
		route: "in",
	}
	inchan <- writemsg
	time.AfterFunc(time.Duration(5)*time.Second, func() {
		quitchan <- true
	})
	for {
		select {
		case err := <-errchan:
			log.Println(err.Error())
		case <-exitchan:
			return
		}
	}

}
