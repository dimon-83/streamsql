package model

import (
	"time"

	"github.com/rulego/streamsql/aggregator"
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
