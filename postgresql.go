// postgresql.go.

////////////////////////////////////////////////////////////////////////////////
//
// Copyright © 2019..2020 by Vault Thirteen.
//
// All rights reserved. No part of this publication may be reproduced,
// distributed, or transmitted in any form or by any means, including
// photocopying, recording, or other electronic or mechanical methods,
// without the prior written permission of the publisher, except in the case
// of brief quotations embodied in critical reviews and certain other
// noncommercial uses permitted by copyright law. For permission requests,
// write to the publisher, addressed “Copyright Protected Material” at the
// address below.
//
////////////////////////////////////////////////////////////////////////////////
//
// Web Site Address:	https://github.com/vault-thirteen.
//
////////////////////////////////////////////////////////////////////////////////

package postgresql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/vault-thirteen/errorz"
	"github.com/vault-thirteen/unicode"
)

// PostgreSQL Constants.
const (
	PostgresqlDsnPrefix                    = "postgresql://"
	PostgresqlDsnUsernamePasswordDelimiter = ":"
	PostgresqlDsnUsernameHostDelimiter     = "@"
	PostgresqlDsnHostPortDelimiter         = ":"
	PostgresqlDsnHostDatabaseDelimiter     = "/"
	PostgresqlDsnParametersPrefix          = "?"
)

const ErrfBadSymbol = "Bad Symbol: '%v'."

// SQL Queries.
const (
	QueryfTableExists = `SELECT EXISTS
(
	SELECT 1
	FROM information_schema.tables
	WHERE
		table_schema = $1 AND
		table_name = $2
);`
	QueryfProcedureExists = `SELECT EXISTS
(
    SELECT 1
    FROM pg_catalog.pg_proc
    JOIN pg_namespace
    ON pg_catalog.pg_proc.pronamespace = pg_namespace.oid
    WHERE
        pg_proc.proname = $1 AND
        pg_namespace.nspname = $2
);`
)

// Symbols.
const (
	SingleQuote      = `'`
	SingleQuoteTwice = SingleQuote + SingleQuote
)

// MakePostgresqlDsn Function returns a Connection String for PostgreSQL
// according to the Documentation at:
// "https://www.postgresql.org/docs/10/libpq-connect.html".
// Format Reference:
// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
func MakePostgresqlDsn(
	host string, // Obligatory Parameter.
	port string, // Obligatory Parameter.
	database string, // Optional Parameter.
	username string, // Optional Parameter.
	password string, // Optional Parameter.

	// Key-Value List without the '?' Prefix.
	// Optional Parameter.
	parameters string,
) string {

	var dsn string

	dsn = PostgresqlDsnPrefix
	if len(username) > 0 {
		if len(password) > 0 {
			dsn = dsn + username + PostgresqlDsnUsernamePasswordDelimiter +
				password + PostgresqlDsnUsernameHostDelimiter
		} else {
			dsn = dsn + username + PostgresqlDsnUsernameHostDelimiter
		}
	}

	dsn = dsn + host + PostgresqlDsnHostPortDelimiter + port

	if len(database) > 0 {
		dsn = dsn + PostgresqlDsnHostDatabaseDelimiter + database
	}

	if len(parameters) > 0 {
		dsn = dsn + PostgresqlDsnParametersPrefix + parameters
	}

	return dsn
}

// TableExists Function checks whether the specified Table exists.
func TableExists(
	connection *sql.DB,
	schemaName string,
	tableName string,
) (result bool, err error) {

	var row *sql.Row
	var statement *sql.Stmt
	var tableExists bool

	statement, err = connection.Prepare(QueryfTableExists)
	if err != nil {
		return false, err
	}
	defer func() {
		var derr error
		derr = statement.Close()
		err = errorz.Combine(err, derr)
	}()

	row = statement.QueryRow(schemaName, tableName)
	err = row.Scan(&tableExists)
	if err != nil {
		return false, err
	}

	return tableExists, nil
}

func TableNameIsGood(
	tableName string,
) (bool, error) {
	return IdentifierIsGood(tableName)
}

func ProcedureNameIsGood(
	procedureName string,
) (bool, error) {
	return IdentifierIsGood(procedureName)
}

func IdentifierIsGood(
	identifierName string,
) (bool, error) {

	for _, letter := range identifierName {
		if (!unicode.SymbolIsLatLetter(letter)) &&
			(!unicode.SymbolIsNumber(letter)) &&
			(letter != '_') {
			return false, fmt.Errorf(ErrfBadSymbol, string(letter))
		}
	}

	return true, nil
}

// ProcedureExists Function checks whether the specified Procedure exists.
func ProcedureExists(
	connection *sql.DB,
	schemaName string,
	procedureName string,
) (result bool, err error) {

	var procedureExists bool
	var row *sql.Row
	var statement *sql.Stmt

	statement, err = connection.Prepare(QueryfProcedureExists)
	if err != nil {
		return false, err
	}
	defer func() {
		var derr error
		derr = statement.Close()
		err = errorz.Combine(err, derr)
	}()

	row = statement.QueryRow(procedureName, schemaName)
	err = row.Scan(&procedureExists)
	if err != nil {
		return false, err
	}

	return procedureExists, nil
}

// ScreenSingleQuotes Function does the Single Quotes Screening.
func ScreenSingleQuotes(
	src string,
) (dst string) {
	return strings.ReplaceAll(src, SingleQuote, SingleQuoteTwice)
}
