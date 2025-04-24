package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBalanceCheck(t *testing.T) {
	exprs := []string{
		"avg(temperature/10)",
		"format_time(window_start(avg,",
		"cast(temperature,  'bigint') as big_temp",
		"lag(temperature) OVER (PARTITION BY deviceId)",
	}
	expected := []bool{
		true,
		false,
		true,
		true,
	}
	for i, expr := range exprs {
		assert.Equal(t, expected[i], IsBalanced(expr))
	}
}

func TestSplitArgs(t *testing.T) {
	exprs := []string{
		"temperature/10",
		" window_start(avg, 'bigint'), 'YYYY-MM-dd HH:mm:ss' ",
		"temperature,  'bigint' ",
	}
	expected := [][]string{
		{"temperature/10"},
		{"window_start(avg, 'bigint')", "YYYY-MM-dd HH:mm:ss"},
		{"temperature", "bigint"},
	}
	for i, expr := range exprs {
		assert.Equal(t, expected[i], SplitArgs(expr))
	}
}
