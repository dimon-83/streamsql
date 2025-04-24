package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseArgs(t *testing.T) {
	expected := []ExprMeta{
		{
			Expression: "deviceId",
			Name:       "deviceId",
			Alias:      "",
			Type:       Field,
		},
		{
			Expression: "avg(temperature/10)",
			Name:       "avg(temperature/10)",
			Alias:      "aa",
			Type:       Expr,
		},
		{
			Expression: "format_time(window_start(avg ,bigint),YYYY-MM-dd HH:mm:ss)",
			Name:       "format_time(window_start(avg ,bigint),YYYY-MM-dd HH:mm:ss)",
			Alias:      "start",
			Type:       Func,
			Args:       []any{"window_start(avg,bigint)", "YYYY-MM-dd HH:mm:ss"},
		},
		{
			Expression: "cast(temperature,  'bigint') as big_temp",
			Name:       "cast(temperature,  'bigint')",
			Alias:      "big_temp",
			Type:       Func,
			Args:       []any{"temperature", "bigint"},
		},
		{
			Expression: "lag(temperature)OVER(PARTITION BY deviceId)",
			Name:       "lag(temperature)",
			Alias:      "",
			Type:       Func,
			Args:       []any{"temperature"},
		},
	}
	exprs := []ExprMeta{
		{
			Expression: "deviceId",
			Name:       "deviceId",
			Type:       Field,
		},
		{
			Expression: "avg(temperature/10)",
			Name:       "avg(temperature/10)",
			Type:       Expr,
		},
		{
			Expression: "format_time(window_start(avg,bigint),YYYY-MM-dd HH:mm:ss)",
			Name:       "format_time(window_start(avg,bigint),YYYY-MM-dd HH:mm:ss)",
			Type:       Func,
		},
		{
			Expression: "cast(temperature,  'bigint') as big_temp",
			Name:       "cast(temperature,  'bigint')",
			Type:       Func,
		},
		{
			Expression: "lag(temperature)OVER(PARTITION BY deviceId)",
			Name:       "lag(temperature)OVER(PARTITION BY deviceId)",
			Type:       Func,
		},
	}

	for i, expr := range exprs {
		expr.ParseArgs()
		assert.Equal(t, expected[i].Args, expr.Args, "Args不匹配，索引：%d, 表达式：%s", i, expr.Expression)
	}

}
