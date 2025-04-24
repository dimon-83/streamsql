package rsql

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/rulego/streamsql/model"
)

type Parser struct {
	lexer *Lexer
}

func NewParser(input string) *Parser {
	return &Parser{
		lexer: NewLexer(input),
	}
}

func (p *Parser) Parse() (*SelectStatement, error) {
	stmt := &SelectStatement{
		Context: model.StreamContext{},
	}

	// 解析SELECT子句
	if err := p.parseSelect(stmt); err != nil {
		return nil, err
	}

	// 解析FROM子句
	if err := p.parseFrom(stmt); err != nil {
		return nil, err
	}

	// 解析WHERE子句
	if err := p.parseWhere(stmt); err != nil {
		return nil, err
	}

	// 解析GROUP BY子句
	if err := p.parseGroupBy(stmt); err != nil {
		return nil, err
	}

	if err := p.parseWith(stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}
func (p *Parser) parseSelect(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过SELECT
	currentToken := p.lexer.NextToken()
	proj := make(model.Projection, 0)
	for {
		var expr strings.Builder
		parenBalance := 0 // 用于跟踪当前表达式片段的括号平衡
		for {
			// 更新括号平衡计数器
			if currentToken.Type == TokenLParen {
				parenBalance++
			} else if currentToken.Type == TokenRParen {
				parenBalance--
			}
			if currentToken.Type == TokenFROM ||
				(currentToken.Type == TokenComma && parenBalance == 0) ||
				currentToken.Type == TokenAS {
				break
			}
			expr.WriteString(currentToken.Value)
			currentToken = p.lexer.NextToken()
		}
		field := Field{Expression: strings.TrimSpace(expr.String())}

		// 处理别名
		if currentToken.Type == TokenAS {
			next := p.lexer.NextToken()
			if next.Type == TokenSpace {
				next = p.lexer.NextToken()
			}
			field.Alias = next.Value
		}
		stmt.Fields = append(stmt.Fields, field)
		if len(field.Expression) > 0 {
			meta := model.ExprMeta{
				Expression: field.Expression,
				Type:       determineExprType(field.Expression),
				Name:       field.Expression,
				Alias:      field.Alias,
			}
			meta.ParseArgs()
			proj = append(proj, meta)
		}
		if currentToken.Type == TokenFROM {
			break
		}
		currentToken = p.lexer.NextToken()
	}
	stmt.Context.Projection = proj
	return nil
}

func (p *Parser) parseWhere(stmt *SelectStatement) error {
	var conditions []string
	current := p.lexer.NextToken() // 跳过WHERE
	if current.Type != TokenWHERE {
		return nil
	}
	for {
		tok := p.lexer.NextToken()
		if tok.Type == TokenGROUP || tok.Type == TokenEOF || tok.Type == TokenSliding ||
			tok.Type == TokenTumbling || tok.Type == TokenCounting || tok.Type == TokenSession {
			break
		}
		switch tok.Type {
		case TokenIdent, TokenNumber:
			conditions = append(conditions, tok.Value)
		case TokenString:
			conditions = append(conditions, "'"+tok.Value+"'")
		case TokenEQ:
			conditions = append(conditions, "==")
		case TokenAND:
			conditions = append(conditions, "&&")
		case TokenOR:
			conditions = append(conditions, "||")
		default:
			// 处理字符串值的引号
			if len(conditions) > 0 && conditions[len(conditions)-1] == "'" {
				conditions[len(conditions)-1] = conditions[len(conditions)-1] + tok.Value
			} else {
				conditions = append(conditions, tok.Value)
			}
		}

	}
	stmt.Condition = strings.Join(conditions, " ")
	return nil
}

func (p *Parser) parseWindowFunction(stmt *SelectStatement, winType string) error {
	p.lexer.NextToken() // 跳过(
	var params []interface{}

	for p.lexer.peekChar() != ')' {
		valTok := p.lexer.NextToken()
		if valTok.Type == TokenRParen || valTok.Type == TokenEOF {
			break
		}
		if valTok.Type == TokenComma {
			continue
		}
		//valTok := p.lexer.NextToken()
		// 处理引号包裹的值
		if strings.HasPrefix(valTok.Value, "'") && strings.HasSuffix(valTok.Value, "'") {
			valTok.Value = strings.Trim(valTok.Value, "'")
		}
		params = append(params, convertValue(valTok.Value))
	}

	if &stmt.Window != nil {
		stmt.Window.Params = params
		stmt.Window.Type = winType
	} else {
		stmt.Window = WindowDefinition{
			Type:   winType,
			Params: params,
		}
	}
	return nil
}

func convertValue(s string) interface{} {
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}
	if i, err := strconv.Atoi(s); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	// 处理引号包裹的字符串
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		return strings.Trim(s, "'")
	}
	return s
}

func (p *Parser) parseFrom(stmt *SelectStatement) error {
	p.lexer.skipWhitespace()
	tok := p.lexer.NextToken()
	if tok.Type != TokenIdent {
		return errors.New("expected source identifier after FROM")
	}
	stmt.Source = tok.Value
	return nil
}

func (p *Parser) parseGroupBy(stmt *SelectStatement) error {
	tok := p.lexer.lookupIdent(p.lexer.readPreviousIdentifier())
	if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
		p.parseWindowFunction(stmt, tok.Value)
	}
	if tok.Type == TokenGROUP {
		p.lexer.NextToken() // 跳过BY
	}

	for {
		p.lexer.skipWhitespace()
		tok := p.lexer.NextToken()
		if tok.Type == TokenWITH || tok.Type == TokenOrder || tok.Type == TokenEOF {
			break
		}
		if tok.Type == TokenComma {
			continue
		}
		if tok.Type == TokenTumbling || tok.Type == TokenSliding || tok.Type == TokenCounting || tok.Type == TokenSession {
			p.parseWindowFunction(stmt, tok.Value)
			continue
		}

		stmt.GroupBy = append(stmt.GroupBy, tok.Value)

		//if p.lexer.NextToken().Type != TokenComma {
		//	break
		//}
	}
	return nil
}

func (p *Parser) parseWith(stmt *SelectStatement) error {
	p.lexer.NextToken() // 跳过(
	for p.lexer.peekChar() != ')' {
		valTok := p.lexer.NextToken()
		if valTok.Type == TokenRParen || valTok.Type == TokenEOF {
			break
		}
		if valTok.Type == TokenComma {
			continue
		}

		if valTok.Type == TokenTimestamp {
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				if strings.HasPrefix(next.Value, "'") && strings.HasSuffix(next.Value, "'") {
					next.Value = strings.Trim(next.Value, "'")
				}
				// 检查Window是否已初始化，如果未初始化则创建新的WindowDefinition
				if stmt.Window.Type == "" {
					stmt.Window = WindowDefinition{
						TsProp: next.Value,
					}
				} else {
					stmt.Window.TsProp = next.Value
				}
			}
		}
		if valTok.Type == TokenTimeUnit {
			timeUnit := time.Minute
			next := p.lexer.NextToken()
			if next.Type == TokenEQ {
				next = p.lexer.NextToken()
				if strings.HasPrefix(next.Value, "'") && strings.HasSuffix(next.Value, "'") {
					next.Value = strings.Trim(next.Value, "'")
				}
				switch next.Value {
				case "dd":
					timeUnit = 24 * time.Hour
				case "hh":
					timeUnit = time.Hour
				case "mi":
					timeUnit = time.Minute
				case "ss":
					timeUnit = time.Second
				case "ms":
					timeUnit = time.Millisecond
				default:

				}
				// 检查Window是否已初始化，如果未初始化则创建新的WindowDefinition
				if stmt.Window.Type == "" {
					stmt.Window = WindowDefinition{
						TimeUnit: timeUnit,
					}
				} else {
					stmt.Window.TimeUnit = timeUnit
				}
			}
		}
	}

	return nil
}

func determineExprType(exprStr string) model.ExprType {
	// 如果包含 OVER 子句，先提取函数部分
	if strings.Contains(strings.ToUpper(exprStr), "OVER") {
		parts := strings.Split(exprStr, "OVER")
		exprStr = strings.TrimSpace(parts[0]) // 只取 OVER 前面的部分进行判断
	}
	program, err := expr.Compile(exprStr)
	if err != nil {
		return model.Field // 默认作为字段处理
	}

	switch node := program.Node().(type) {
	case *ast.IdentifierNode:
		// 处理简单字段，如 deviceId
		return model.Field

	case *ast.CallNode:
		// 检查是否是窗口函数
		windowTypes := []string{"TumblingWindow", "SlidingWindow", "SessionWindow"}
		for _, wt := range windowTypes {
			if node.Callee.(*ast.IdentifierNode).Value == wt {
				return model.Win
			}
		}
		// 其他函数调用，如 format_time(), avg() 等
		return model.Func

	case *ast.BinaryNode:
		// 处理表达式，如 temperature/10, a+b+c/d
		return model.Expr

	default:
		return model.Field
	}
}
