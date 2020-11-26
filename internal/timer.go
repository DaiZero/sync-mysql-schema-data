package internal

import (
	"fmt"
	"time"
)

//myTimer 计算器
type myTimer struct {
	start time.Time
	end   time.Time
}

//newMyTimer 新建一个计时器，并且赋值开始时间
func newMyTimer() *myTimer {
	return &myTimer{
		start: time.Now(),
	}
}

//stop 停止设计器（赋值结束时间）
func (mt *myTimer) stop() {
	mt.end = time.Now()
}

//usedSecond 输出计算器时长，单位秒
func (mt *myTimer) usedSecond() string {
	return fmt.Sprintf("%f s", mt.end.Sub(mt.start).Seconds())
}
