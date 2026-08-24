package core

const defaultFetchMax = 500

// latestStart 计算「最新 n 条」的起始 offset，并夹到 [oldest, hwm]。
func latestStart(oldest, hwm, n int64) int64 {
	if n <= 0 {
		n = 50
	}
	if hwm <= oldest {
		return oldest
	}
	start := hwm - n
	if start < oldest {
		return oldest
	}
	return start
}

// normalizeFetchRange 将用户输入规范为半开区间 [from, toEx)。
//
//   - oldest / hwm：分区当前可消费范围（hwm 为下一条写入 offset）
//   - start：起始 offset（包含）
//   - end：结束 offset（包含）；<=0 表示读到最新（toEx = hwm）
//   - max：最多条数
func normalizeFetchRange(oldest, hwm, start, end int64, max int) (from, toEx int64) {
	if max <= 0 {
		max = defaultFetchMax
	}
	if oldest < 0 {
		oldest = 0
	}
	if start < oldest {
		start = oldest
	}
	if end <= 0 {
		toEx = hwm
	} else {
		toEx = end + 1
		if toEx > hwm {
			toEx = hwm
		}
	}
	if start >= hwm || start >= toEx {
		return start, start
	}
	if toEx-start > int64(max) {
		toEx = start + int64(max)
	}
	return start, toEx
}
