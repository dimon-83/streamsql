package rsql

import (
	"testing"
	"time"

	"github.com/rulego/streamsql/aggregator"
	"github.com/rulego/streamsql/model"

	"github.com/stretchr/testify/assert"
)

func TestParseSQL(t *testing.T) {
	tests := []struct {
		sql       string
		expected  *model.Config
		condition string
	}{
		{
			sql: "select deviceId, avg(temperature/10) as aa from Input where deviceId='aa' group by deviceId, TumblingWindow('10s')",
			expected: &model.Config{
				WindowConfig: model.WindowConfig{
					Type: "tumbling",
					Params: map[string]interface{}{
						"size": 10 * time.Second,
					},
				},
				GroupFields: []string{"deviceId"},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": "avg",
				},
				FieldAlias: map[string]string{
					"temperature": "aa",
				},
			},
			condition: "deviceId == 'aa'",
		},
		{
			sql: "select max(humidity) as max_humidity, min(temperature) as min_temp from Sensor group by type, SlidingWindow('20s', '5s')",
			expected: &model.Config{
				WindowConfig: model.WindowConfig{
					Type: "sliding",
					Params: map[string]interface{}{
						"size":  20 * time.Second,
						"slide": 5 * time.Second,
					},
				},
				GroupFields: []string{"type"},
				SelectFields: map[string]aggregator.AggregateType{
					"humidity":    "max",
					"temperature": "min",
				},
			},
			condition: "",
		},
		{
			sql: "select deviceId, avg(temperature/10) as aa from Input where deviceId='aa' group by TumblingWindow('10s'), deviceId  with (TIMESTAMP='ts') ",
			expected: &model.Config{
				WindowConfig: model.WindowConfig{
					Type: "tumbling",
					Params: map[string]interface{}{
						"size": 10 * time.Second,
					},
					TsProp: "ts",
				},
				GroupFields: []string{"deviceId"},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": "avg",
				},
				FieldAlias: map[string]string{
					"temperature": "aa",
				},
			},
			condition: "deviceId == 'aa'",
		},
		{
			sql: "select deviceId, avg(temperature/10) as aa from Input where deviceId='aa' and temperature>0  TumblingWindow('10s') with (TIMESTAMP='ts') ",
			expected: &model.Config{
				WindowConfig: model.WindowConfig{
					Type: "tumbling",
					Params: map[string]interface{}{
						"size": 10 * time.Second,
					},
					TsProp: "ts",
				},
				SelectFields: map[string]aggregator.AggregateType{
					"temperature": "avg",
				},
				FieldAlias: map[string]string{
					"temperature": "aa",
				},
			},
			condition: "deviceId == 'aa' && temperature > 0",
		},
	}

	for _, tt := range tests {
		parser := NewParser(tt.sql)
		stmt, err := parser.Parse()
		assert.NoError(t, err)

		config, cond, err := stmt.ToStreamConfig()
		assert.NoError(t, err)

		assert.Equal(t, tt.expected.WindowConfig.Type, config.WindowConfig.Type)
		assert.Equal(t, tt.expected.WindowConfig.Params["size"], config.WindowConfig.Params["size"])
		assert.Equal(t, tt.expected.GroupFields, config.GroupFields)
		assert.Equal(t, tt.expected.SelectFields, config.SelectFields)
		assert.Equal(t, tt.condition, cond)
		if tt.expected.WindowConfig.TsProp != "" {
			assert.Equal(t, tt.expected.WindowConfig.TsProp, config.WindowConfig.TsProp)
		}
	}
}
func TestWindowParamParsing(t *testing.T) {
	params := []interface{}{"10s", "5s"}
	result, err := parseWindowParams(params)
	assert.NoError(t, err)
	assert.Equal(t, 10*time.Second, result["size"])
	assert.Equal(t, 5*time.Second, result["slide"])
}

func TestConditionParsing(t *testing.T) {
	sql := "select cpu,mem from metrics where cpu > 80 or (mem < 20 and disk == '/dev/sda')"
	expected := "cpu > 80 || ( mem < 20 && disk == '/dev/sda' )"

	parser := NewParser(sql)
	stmt, err := parser.Parse()
	assert.NoError(t, err)
	assert.Equal(t, expected, stmt.Condition)
}

func TestDetermineExprType(t *testing.T) {
	tests := []struct {
		name     string
		exprStr  string
		expected model.ExprType
	}{
		{
			name:     "简单字段",
			exprStr:  "deviceId",
			expected: model.Field,
		},
		{
			name:     "聚合函数",
			exprStr:  "avg(temperature/10)",
			expected: model.Func,
		},
		{
			name:     "格式化函数",
			exprStr:  "format_time(window_start(),'YYYY-MM-dd HH:mm:ss')",
			expected: model.Func,
		},
		{
			name:     "窗口函数",
			exprStr:  "lag(temperature)OVER(PARTITION BY deviIdce)",
			expected: model.Func,
		},
		{
			name:     "滚动窗口函数",
			exprStr:  "TumblingWindow('10s')",
			expected: model.Win,
		},
		{
			name:     "滑动窗口函数",
			exprStr:  "SlidingWindow('1m')",
			expected: model.Win,
		},
		{
			name:     "会话窗口函数",
			exprStr:  "SessionWindow('30s')",
			expected: model.Win,
		},
		{
			name:     "二元表达式",
			exprStr:  "temperature/10",
			expected: model.Expr,
		},
		{
			name:     "复杂二元表达式",
			exprStr:  "a+b+c/d",
			expected: model.Expr,
		},
		{
			name:     "类型转换函数",
			exprStr:  "cast(temperature,bigint)",
			expected: model.Func,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineExprType(tt.exprStr)
			assert.Equal(t, tt.expected, result, "表达式 '%s' 的类型判断错误", tt.exprStr)
		})
	}
}

func TestParseProjection(t *testing.T) {
	tests := []struct {
		sql      string
		expected []model.ExprMeta
	}{
		{
			sql: "select deviceId, avg(temperature/10) as aa," +
				" format_time(window_start(), 'YYYY-MM-dd HH:mm:ss') as start , " +
				" cast(temperature,  'bigint') as big_temp, " +
				" lag(temperature) OVER (PARTITION BY deviceId) " +
				" from Input where deviceId='aa' group by deviceId, TumblingWindow('10s')" +
				" Order By start desc,deviceId asc " +
				" having avg(temperature/10) > 5 ",
			expected: []model.ExprMeta{
				{
					Expression: "deviceId",
					Name:       "deviceId",
					Alias:      "",
					Type:       model.Field,
				},
				{
					Expression: "avg(temperature/10)",
					Name:       "avg(temperature/10)",
					Alias:      "aa",
					Type:       model.Func,
				},
				{
					Expression: "format_time(window_start(), 'YYYY-MM-dd HH:mm:ss')",
					Name:       "format_time(window_start(), 'YYYY-MM-dd HH:mm:ss')",
					Alias:      "start",
					Type:       model.Func,
					Args:       []any{"window_start()", "YYYY-MM-dd HH:mm:ss"},
				},
				{
					Expression: "cast(temperature, 'bigint')",
					Name:       "cast(temperature, 'bigint')",
					Alias:      "big_temp",
					Type:       model.Func,
					Args:       []any{"temperature", "bigint"},
				},
				{
					Expression: "lag(temperature) OVER (PARTITION BY deviceId)",
					Name:       "lag(temperature)",
					Alias:      "",
					Type:       model.Func,
					Args:       []any{"temperature"},
				},
			},
		},
	}

	for _, tt := range tests {
		parser := NewParser(tt.sql)
		stmt, err := parser.Parse()
		assert.NoError(t, err)

		actcualProjection := stmt.Context.Projection
		assert.Equal(t, len(tt.expected), len(actcualProjection))
		for i, expected := range tt.expected {
			assert.Equal(t, expected.Expression, actcualProjection[i].Expression)
			assert.Equal(t, expected.Name, actcualProjection[i].Name)
			assert.Equal(t, expected.Alias, actcualProjection[i].Alias)
			assert.Equal(t, expected.Type, actcualProjection[i].Type)
			if expected.Args != nil {
				assert.Equal(t, expected.Args, actcualProjection[i].Args)
			}
			if expected.OverClause != nil {
				assert.Equal(t, expected.OverClause, actcualProjection[i].OverClause)
			}

		}

	}
}
