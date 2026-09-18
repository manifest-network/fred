// Package fsidentity provides retained Linux directory capabilities for
// security-sensitive filesystem publication. A Directory binds one physical
// device/inode and performs entry operations relative to its open descriptor,
// preventing pathname replacement from redirecting those operations.
package fsidentity
