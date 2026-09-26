// Package credentials gives instance manager commands the passwords of the
// MySQL accounts they drive. In a Pod they come from the cluster's credential
// Secrets through the Kubernetes API (design 030); outside Kubernetes (the
// integration tests) from the legacy MYSQL_*_PASSWORD environment variables.
package credentials

import (
	"errors"
	"os"
)

// Account names one MySQL account whose password the instance manager needs.
type Account string

const (
	Root    Account = "root"
	App     Account = "app"
	Control Account = "control"
	Backup  Account = "backup"
	Dump    Account = "dump"
)

var (
	// ErrUnknown means this cluster has no Secret for the account.
	ErrUnknown = errors.New("no credential secret for this account")
	// ErrNotLoaded means the account's Secret has not been read yet.
	ErrNotLoaded = errors.New("credential secret not read yet")
)

// Source hands out the current password of an account.
type Source interface {
	Password(a Account) (string, error)
}

// Static is a fixed set of passwords.
type Static map[Account]string

func (s Static) Password(a Account) (string, error) {
	if p, ok := s[a]; ok {
		return p, nil
	}
	return "", ErrUnknown
}

var envNames = map[Account]string{
	Root:    "MYSQL_ROOT_PASSWORD",
	App:     "MYSQL_APP_PASSWORD",
	Control: "MYSQL_CONTROL_PASSWORD",
	Backup:  "MYSQL_BACKUP_PASSWORD",
	// MYSQL_DUMP_PASSWORD is new: the dump account's password was previously
	// carried by the backup worker, so env mode had no name for it. This lets
	// standalone runs and the Docker integration tests still serve dumps.
	Dump: "MYSQL_DUMP_PASSWORD",
}

// FromEnv reads the legacy MYSQL_*_PASSWORD variables (the dump account comes
// from MYSQL_DUMP_PASSWORD). Unset ones are absent.
func FromEnv() Static {
	s := Static{}
	for a, name := range envNames {
		if v, ok := os.LookupEnv(name); ok {
			s[a] = v
		}
	}
	return s
}

// Getter adapts a Source to the func() string long-lived consumers call before
// each use. An unavailable password reads as empty, which the consumer reports.
func Getter(s Source, a Account) func() string {
	return func() string {
		p, _ := s.Password(a)
		return p
	}
}
