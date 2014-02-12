package blocks

import (
	"errors"
	"time"
)

// emits the time. Specify the period - the time between emissions - in seconds
// as a rule.
type Ticker struct {
	Block
	ticker <-chan time.Time
	Kind   string "ticker"
}

func (b *Ticker) setup() error {
	b.Rule = map[string]interface{}{
		"Interval": "1s",
	}
	b.ticker = time.Tick(time.Duration(1) * time.Second)
	return nil
}

func (b *Ticker) EmitMessages(outchan chan MsgData) {

	for {
		select {
		case tick := <-b.ticker:
			out := map[string]interface{}{
				"time": tick,
			}
			outchan <- out
		}
	}
}

func (b *Ticker) setRule(msg MsgData) error {
	intervalInterface, ok := msg["Interval"]
	if !ok {
		return errors.New("Rule message did not contain Interval")
	}
	interval, ok := intervalInterface.(string)
	if !ok {
		return errors.New("Interval could not be type asserted to string")
	}
	newDur, err := time.ParseDuration(interval)
	if err != nil {
		return err
	}
	b.ticker = time.Tick(newDur)
	return nil

}
