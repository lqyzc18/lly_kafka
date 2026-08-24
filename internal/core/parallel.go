package core

import "sync"

const defaultConcurrency = 16

// runBounded 以最多 bound 个 goroutine 并行执行 fn(0..n-1)。
func runBounded(n, bound int, fn func(i int)) {
	if n <= 0 {
		return
	}
	if bound <= 0 {
		bound = defaultConcurrency
	}
	sem := make(chan struct{}, bound)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}()
	}
	wg.Wait()
}
