package udp

import (
	"errors"
	"github.com/raintank/statsdaemon/common"
	"strconv"
)

type lexer struct {
	input []byte
	len   int
	start int
	pos   int
	m     common.Metric
	err   error
}

// assumes we don't have \x00 bytes in input
const eof = 0

func (l *lexer) next() byte {
	if l.pos >= l.len {
		return eof
	}
	b := l.input[l.pos]
	l.pos++
	return b
}

func (l *lexer) run() {
	for state := lexKeySep; state != nil; {
		state = state(l)
	}
	if l.err == nil && l.m.Sampling == 0 {
		l.m.Sampling = float32(1)
	}

}

var (
	errMissingKeySep   = errors.New("missing key separator")
	errEmptyKey        = errors.New("key zero len")
	errMissingValueSep = errors.New("missing value separator")
	errInvalidModifier = errors.New("invalid modifier")
	errInvalidSampling = errors.New("invalid sampling")
)

type stateFn func(*lexer) stateFn

// lex until we find the colon separator between key and value
func lexKeySep(l *lexer) stateFn {
	for {
		switch b := l.next(); b {
		case ':':
			return lexKey
		case eof:
			l.err = errMissingKeySep
			return nil
		}
	}
}

// lex the key
func lexKey(l *lexer) stateFn {
	if l.start == l.pos-1 {
		l.err = errEmptyKey
		return nil
	}
	l.m.Bucket = string(l.input[l.start : l.pos-1])
	l.start = l.pos
	return lexValueSep
}

// lex until we find the pipe separator between value and modifier
func lexValueSep(l *lexer) stateFn {
	for {
		// cheap check here. ParseFloat will do it.
		switch b := l.next(); b {
		case '|':
			return lexValue
		case eof:
			l.err = errMissingValueSep
			return nil
		}
	}
}

// lex the value
func lexValue(l *lexer) stateFn {
	v, err := strconv.ParseFloat(string(l.input[l.start:l.pos-1]), 64)
	if err != nil {
		l.err = err
		return nil
	}
	l.m.Value = v
	l.start = l.pos
	return lexModifier
}

// lex the modifier
func lexModifier(l *lexer) stateFn {
	b := l.next()
	switch b {
	case 'g':
		fallthrough
	case 'c':
		l.m.Modifier = string(b)
		l.start = l.pos
		return lexModifierSep
	case 'm':
		if b := l.next(); b != 's' {
			l.err = errInvalidModifier
			return nil
		}
		l.start = l.pos
		l.m.Modifier = "ms"
		return lexModifierSep
	default:
		l.err = errInvalidModifier
		return nil

	}
}

// lex the possible separator between modifier and samplerate
func lexModifierSep(l *lexer) stateFn {
	b := l.next()
	switch b {
	case eof:
		return nil
	case '|':
		l.start = l.pos
		return lexSampleRate
	}
	l.err = errInvalidModifier
	return nil
}

// lex the sample rate
func lexSampleRate(l *lexer) stateFn {
	b := l.next()
	if b != '@' {
		l.err = errInvalidSampling
	}
	l.start = l.pos

	v, err := strconv.ParseFloat(string(l.input[l.start:]), 32)
	if err != nil {
		l.err = err
		return nil
	}
	l.m.Sampling = float32(v)
	return nil
}

// ParseLine with lexer impl
func ParseLine2(line []byte) (*common.Metric, error) {
	llen := len(line)
	if llen == 0 {
		return nil, nil
	}
	l := &lexer{input: line, len: llen}
	l.run()
	if l.err != nil {
		return nil, l.err
	}
	return &l.m, nil
}
