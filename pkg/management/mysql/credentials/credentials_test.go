package credentials

import (
	"errors"
	"testing"
)

func TestStaticPassword(t *testing.T) {
	s := Static{Control: "c"}
	if p, err := s.Password(Control); err != nil || p != "c" {
		t.Fatalf("Password(Control) = %q, %v", p, err)
	}
	if _, err := s.Password(Dump); !errors.Is(err, ErrUnknown) {
		t.Fatalf("Password(Dump) err = %v, want ErrUnknown", err)
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("MYSQL_ROOT_PASSWORD", "r")
	t.Setenv("MYSQL_CONTROL_PASSWORD", "c")
	t.Setenv("MYSQL_REPLICATION_PASSWORD", "rep") // no longer an account: must be ignored
	t.Setenv("CNMSQL_DUMP_PASSWORD", "legacy")    // the deleted worker variable is not a source
	t.Setenv("MYSQL_DUMP_PASSWORD", "d")
	s := FromEnv()
	if s[Root] != "r" || s[Control] != "c" || s[Dump] != "d" || len(s) != 3 {
		t.Fatalf("FromEnv = %v", s)
	}
	if _, ok := s[Backup]; ok {
		t.Fatal("unset env vars must be absent, not empty")
	}
}

func TestGetterSwallowsErrors(t *testing.T) {
	if got := Getter(Static{}, Control)(); got != "" {
		t.Fatalf("Getter on missing account = %q", got)
	}
}
