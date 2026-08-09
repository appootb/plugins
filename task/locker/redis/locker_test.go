package redis

import (
	"testing"
)

func TestLockerKey(t *testing.T) {
	if got := lockerKey("daily-report"); got != "task:scheduler:locker:daily-report" {
		t.Fatalf("got %q", got)
	}
}

func TestInitComponent(t *testing.T) {
	prev := impl.component
	t.Cleanup(func() { impl.component = prev })
	InitComponent("task-redis")
	if impl.component != "task-redis" {
		t.Fatalf("component = %q", impl.component)
	}
}

func TestUnlock_MissingIsNoop(t *testing.T) {
	l := &locker{}
	l.Unlock("never-locked") // should not panic
}

func TestConstants(t *testing.T) {
	if RandomValueLength <= 0 {
		t.Fatal("RandomValueLength")
	}
	if LockerTouchTimeout <= 0 {
		t.Fatal("LockerTouchTimeout")
	}
}
