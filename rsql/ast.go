package rsql

import (
	"fmt"
	"strings"
	"time"

	"github.com/rulego/streamsql/model"
	"github.com/rulego/streamsql/window"

	"github.com/rulego/streamsql/aggregator"
)

type SelectStatement struct {
	Fields    []Field
	Source    string
	Condition string
	Window    WindowDefinition
	GroupBy   []string
}

type Field struct {
	Expression string
	Alias      string
}

type WindowDefinition struct {
	Type     string
	Params   []interface{}
	TsProp   string
	TimeUnit time.Duration
}

// ToStreamConfig 将AST转换为Stream配置
func (s *SelectStatement) ToStreamConfig() (*model.Config, string, error) {
	if s.Source == "" {
		return nil, "", fmt.Errorf("missing FROM clause")
	}
	// 解析窗口配置
	windowType := window.TypeTumbling
	if strings.ToUpper(s.Window.Type) == "TUMBLINGWINDOW" {
		windowType = window.TypeTumbling
	} else if strings.ToUpper(s.Window.Type) == "SLIDINGWINDOW" {
		windowType = window.TypeSliding
	} else if strings.ToUpper(s.Window.Type) == "COUNTINGWINDOW" {
		windowType = window.TypeCounting
	} else if strings.ToUpper(s.Window.Type) == "SESSIONWINDOW" {
		windowType = window.TypeSession
	}

	params, err := parseWindowParams(s.Window.Params)
	if err != nil {
		return nil, "", fmt.Errorf("解析窗口参数失败: %w", err)
	}

	// 构建Stream配置
	config := model.Config{
		WindowConfig: model.WindowConfig{
			Type:     windowType,
			Params:   params,
			TsProp:   s.Window.TsProp,
			TimeUnit: s.Window.TimeUnit,
		},
		GroupFields:  extractGroupFields(s),
		SelectFields: buildSelectFields(s.Fields),
	}

	return &config, s.Condition, nil
}

func extractGroupFields(s *SelectStatement) []string {
	var fields []string
	for _, f := range s.GroupBy {
		if !strings.Contains(f, "(") { // 排除聚合函数
			fields = append(fields, f)
		}
	}
	return fields
}

func buildSelectFields(fields []Field) map[string]aggregator.AggregateType {
	selectFields := make(map[string]aggregator.AggregateType)
	for _, f := range fields {
		if alias := f.Alias; alias != "" {
			selectFields[alias] = parseAggregateType(f.Expression)
		}
	}
	return selectFields
}

func parseAggregateType(expr string) aggregator.AggregateType {
	if strings.Contains(expr, "avg(") {
		return "avg"
	}
	if strings.Contains(expr, "sum(") {
		return "sum"
	}
	if strings.Contains(expr, "max(") {
		return "max"
	}
	if strings.Contains(expr, "min(") {
		return "min"
	}
	return ""
}

func parseWindowParams(params []interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	var key string
	for index, v := range params {
		if index == 0 {
			key = "size"
		} else if index == 1 {
			key = "slide"
		} else {
			key = "offset"
		}
		if s, ok := v.(string); ok {
			dur, err := time.ParseDuration(s)
			if err != nil {
				return nil, fmt.Errorf("invalid %s duration: %w", s, err)
			}
			result[key] = dur
		} else {
			return nil, fmt.Errorf("%s参数必须为字符串格式(如'5s')", s)
		}
	}

	return result, nil
}

func parseAggregateExpression(expr string) string {
	if strings.Contains(expr, "avg(") {
		return "avg"
	}
	if strings.Contains(expr, "sum(") {
		return "sum"
	}
	if strings.Contains(expr, "max(") {
		return "max"
	}
	if strings.Contains(expr, "min(") {
		return "min"
	}
	return ""
}
