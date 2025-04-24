package rsql

import "strings"

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenIdent
	TokenNumber
	TokenString
	TokenComma
	TokenLParen
	TokenRParen
	TokenPlus
	TokenMinus
	TokenAsterisk
	TokenSlash
	TokenEQ
	TokenStrEQ
	TokenNE
	TokenGT
	TokenLT
	TokenGE
	TokenLE
	TokenAND
	TokenOR
	TokenSELECT
	TokenFROM
	TokenWHERE
	TokenGROUP
	TokenBY
	TokenAS
	TokenTumbling
	TokenSliding
	TokenCounting
	TokenSession
	TokenWITH
	TokenTimestamp
	TokenTimeUnit
	TokenOrder
	TokenSpace
)

type Token struct {
	Type  TokenType
	Value string
	Pos   int
}

type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
	cuurent Token
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) NextToken() Token {
	// 检查是否有空格，如果有则返回空格标记
	if isWhitespace(l.ch) {
		return l.readSpace()
	}

	switch l.ch {
	case 0:
		l.cuurent = Token{Type: TokenEOF}
		return l.cuurent
	case ',':
		l.readChar()
		l.cuurent = Token{Type: TokenComma, Value: ","}
		return l.cuurent
	case '(':
		l.readChar()
		l.cuurent = Token{Type: TokenLParen, Value: "("}
		return l.cuurent
	case ')':
		l.readChar()
		l.cuurent = Token{Type: TokenRParen, Value: ")"}
		return l.cuurent
	case '+':
		l.readChar()
		l.cuurent = Token{Type: TokenPlus, Value: "+"}
		return l.cuurent
	case '-':
		l.readChar()
		l.cuurent = Token{Type: TokenMinus, Value: "-"}
		return l.cuurent
	case '*':
		l.readChar()
		l.cuurent = Token{Type: TokenAsterisk, Value: "*"}
		return l.cuurent
	case '/':
		l.readChar()
		l.cuurent = Token{Type: TokenSlash, Value: "/"}
		return l.cuurent
	case '=':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			l.cuurent = Token{Type: TokenEQ, Value: "=="}
			return l.cuurent
		}
		l.readChar()
		l.cuurent = Token{Type: TokenEQ, Value: "="}
		return l.cuurent
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			l.cuurent = Token{Type: TokenGE, Value: ">="}
			return l.cuurent
		}
		l.readChar()
		l.cuurent = Token{Type: TokenGT, Value: ">"}
		return l.cuurent
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			l.cuurent = Token{Type: TokenLE, Value: "<="}
			return l.cuurent
		}
		l.readChar()
		l.cuurent = Token{Type: TokenLT, Value: "<"}
		return l.cuurent
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			l.cuurent = Token{Type: TokenStrEQ, Value: "!="}
			return l.cuurent
		}
	}

	if isLetter(l.ch) {
		ident := l.readIdentifier()
		l.cuurent = l.lookupIdent(ident)
		return l.cuurent
	}

	if isDigit(l.ch) {
		l.cuurent = Token{Type: TokenNumber, Value: l.readNumber()}
		return l.cuurent
	}

	if l.ch == '\'' {
		l.cuurent = Token{Type: TokenString, Value: l.readString()}
		return l.cuurent
	}

	l.readChar()
	l.cuurent = Token{Type: TokenEOF}
	return l.cuurent
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

func (l *Lexer) readIdentifier() string {
	pos := l.pos
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readPreviousIdentifier() string {
	// 保存当前位置
	endPos := l.pos

	// 向前移动直到找到非字母字符或到达输入开始
	startPos := endPos - 1
	for startPos >= 0 && isLetter(l.input[startPos]) {
		startPos--
	}

	// 调整到第一个字母字符的位置
	startPos++

	// 如果找到有效的标识符，返回它
	if startPos < endPos {
		return l.input[startPos:endPos]
	}

	return ""
}

func (l *Lexer) readNumber() string {
	pos := l.pos
	for isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readString() string {
	pos := l.pos
	l.readChar()
	for {
		if l.ch == '\'' {
			l.readChar()
			break
		}
		l.readChar()
	}

	str := l.input[pos:l.pos]
	return str
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) lookupIdent(ident string) Token {
	switch strings.ToUpper(ident) {
	case "SELECT":
		return Token{Type: TokenSELECT, Value: ident}
	case "FROM":
		return Token{Type: TokenFROM, Value: ident}
	case "WHERE":
		return Token{Type: TokenWHERE, Value: ident}
	case "GROUP":
		return Token{Type: TokenGROUP, Value: ident}
	case "BY":
		return Token{Type: TokenBY, Value: ident}
	case "AS":
		return Token{Type: TokenAS, Value: ident}
	case "OR":
		return Token{Type: TokenOR, Value: ident}
	case "AND":
		return Token{Type: TokenAND, Value: ident}
	case "TUMBLINGWINDOW":
		return Token{Type: TokenTumbling, Value: ident}
	case "SLIDINGWINDOW":
		return Token{Type: TokenSliding, Value: ident}
	case "COUNTINGWINDOW":
		return Token{Type: TokenCounting, Value: ident}
	case "SESSIONWINDOW":
		return Token{Type: TokenSession, Value: ident}
	case "WITH":
		return Token{Type: TokenWITH, Value: ident}
	case "TIMESTAMP":
		return Token{Type: TokenTimestamp, Value: ident}
	case "TIMEUNIT":
		return Token{Type: TokenTimeUnit, Value: ident}
	case "ORDER":
		return Token{Type: TokenOrder, Value: ident}
	default:
		return Token{Type: TokenIdent, Value: ident}
	}
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// 判断字符是否为空白字符
func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

// 新增方法：读取并压缩连续空格
func (l *Lexer) readSpace() Token {
	// 读取所有连续的空白字符
	for isWhitespace(l.ch) {
		l.readChar()
	}

	// 无论有多少连续空格，都返回一个单空格标记
	l.cuurent = Token{Type: TokenSpace, Value: " "}
	return l.cuurent
}
