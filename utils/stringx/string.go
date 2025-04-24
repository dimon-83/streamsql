package utils

import (
	"strings"
)

// IsBalanced 检查字符串中的括号是否平衡
func IsBalanced(s string) bool {
	balance := 0 // 计数器，'(' 加 1，')' 减 1
	for _, r := range s {
		if r == '(' {
			balance++
		} else if r == ')' {
			balance--
		}
		// 如果在任何时候 balance < 0，说明出现了未匹配的 ')'
		if balance < 0 {
			return false
		}
	}
	// 循环结束后，只有当 balance 为 0 时，括号才是完全匹配的
	return balance == 0
}

// SplitArgs 分割字符串，保留括号内的逗号
func SplitArgs(s string) []string {
	var result []string
	var currentPart strings.Builder

	startIndex := 0
	for i := 0; i < len(s); i++ {
		// 找到逗号
		if s[i] == ',' {
			// 检查从开始到当前位置的子字符串是否括号平衡
			currentPart.WriteString(s[startIndex:i])
			if IsBalanced(currentPart.String()) {
				// 如果平衡，则添加到结果中并更新开始索引
				result = append(result, strings.TrimSpace(currentPart.String()))
				startIndex = i + 1 // 从逗号后一位开始
			}
			currentPart.Reset()
		}
	}

	// 处理最后一部分
	if startIndex < len(s) {
		lastPart := s[startIndex:]
		result = append(result, strings.TrimSpace(lastPart))
	}

	for i := range result {
		arg := strings.TrimSpace(result[i])
		if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
			arg = arg[1 : len(arg)-1]
		}
		result[i] = arg
	}

	return result
}
