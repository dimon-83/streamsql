package model

import (
	"regexp"
	"strings"
	"time"

	"github.com/rulego/streamsql/aggregator"
	stringx "github.com/rulego/streamsql/utils/stringx"
)

type ExprType int
type OrderType int

const (
	Field ExprType = iota
	Expr
	Func
	Win

	ASC OrderType = iota
	DESC
)

type Config struct {
	WindowConfig WindowConfig
	GroupFields  []string
	SelectFields map[string]aggregator.AggregateType
	FieldAlias   map[string]string
}
type WindowConfig struct {
	Type     string
	Params   map[string]interface{}
	TsProp   string
	TimeUnit time.Duration
}

type ExprMeta struct {
	Expression string      // 原始表达式 ，如：avg(price)
	Name       string      // 名称: filed对应字段名，expr对应表达式，func、window对应函数名
	Alias      string      // 别名
	Type       ExprType    // 类型: filed , expr , func , window
	Args       []any       // 函数参数
	Sort       OrderType   // 排序: 0:升序, 1: 降序
	OverClause *OverClause // over 子句
}

func (e *ExprMeta) ParseArgs() {
	// 仅当类型为函数才解析参数
	if e.Type != Func {
		return
	}
	//TODO 处理over 子句
	overClauseRegex := regexp.MustCompile(`(?i)OVER\s*\(.*\).*$`)
	argsStr := strings.TrimSpace(overClauseRegex.ReplaceAllString(e.Expression, ""))
	e.Name = argsStr
	// 查找第一个左括号和最后一个右括号
	firstParen := strings.Index(argsStr, "(")
	lastParen := strings.LastIndex(argsStr, ")")
	// 确保括号存在且成对出现（基础检查）
	if firstParen == -1 || lastParen == -1 || firstParen >= lastParen {
		return // 没有找到有效的括号对
	}
	argsStr = strings.TrimSpace(argsStr[firstParen+1 : lastParen])

	// 按逗号分割参数
	args := stringx.SplitArgs(argsStr) // 自定义的函数，用于分割参数，确保括号平衡
	// 将 []string 转换为 []any
	e.Args = make([]any, len(args))
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
			arg = arg[1 : len(arg)-1]
		}
		e.Args[i] = arg

	}
}

type Projection []ExprMeta
type PartitionBy []ExprMeta
type GroupBy []ExprMeta
type OrderBy []ExprMeta
type Having []ExprMeta
type Window ExprMeta
type With ExprMeta

type OverClause struct {
	PartitionBy PartitionBy
	OrderBy     OrderBy
}

type StreamContext struct {
	Projection
	GroupBy
	OrderBy
	Having
	Window
	With
}
