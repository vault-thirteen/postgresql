////////////////////////////////////////////////////////////////////////////////
//
// Copyright © 2019 by Vault Thirteen.
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

// +build test

package postgresql

import (
	"database/sql"
	"fmt"

	// PostgreSQL Driver.
	_ "github.com/lib/pq"

	"github.com/vault-thirteen/errorz"
)

// Test Database Parameters.
const (
	TestDatabaseDriver     = "postgres"
	TestDatabaseHost       = "localhost"
	TestDatabasePort       = "5432"
	TestDatabaseDatabase   = "test"
	TestDatabaseUser       = "test"
	TestDatabasePassword   = "test"
	TestDatabaseParameters = "sslmode=disable"
)

// Schema Names.
const (
	SchemaCommon = "public"
)

// Table Names.
const (
	TableNameExistent    = "TableA"
	TableNameNotExistent = "xxxxxxxxx"
)

// Procedure Names.
const (
	ProcedureNameExistent    = "procedure_simulator"
	ProcedureNameNotExistent = "xxxxxxxxx"
)

func makeTestDatabaseDsn() (dsn string) {
	dsn = MakePostgresqlDsn(
		TestDatabaseHost,
		TestDatabasePort,
		TestDatabaseDatabase,
		TestDatabaseUser,
		TestDatabasePassword,
		TestDatabaseParameters,
	)
	return
}

func connectToTestDatabase(
	dsn string,
) (sqlConnection *sql.DB, err error) {
	return sql.Open(TestDatabaseDriver, dsn)
}

func createTestTable() (err error) {

	const QueryfCreateTable = `CREATE TABLE IF NOT EXISTS %v
(
	"Id" serial
);`

	var query string
	var sqlConnection *sql.DB

	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	if err != nil {
		return
	}
	defer func() {
		var derr = sqlConnection.Close()
		err = errorz.Combine(err, derr)
	}()

	// Create the Table.
	query = fmt.Sprintf(
		QueryfCreateTable,
		fmt.Sprintf(`%s."%s"`,
			SchemaCommon,
			TableNameExistent,
		),
	)
	_, err = sqlConnection.Exec(query)
	if err != nil {
		return
	}
	return
}

func createTestProcedure() (err error) {

	const QueryfCreateProcedure = `CREATE OR REPLACE PROCEDURE %v()
LANGUAGE SQL
AS
$$
	SELECT 123
$$;`

	var query string
	var sqlConnection *sql.DB

	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	if err != nil {
		return
	}
	defer func() {
		var derr = sqlConnection.Close()
		err = errorz.Combine(err, derr)
	}()

	// Create the Table.
	query = fmt.Sprintf(
		QueryfCreateProcedure,
		ProcedureNameExistent,
	)
	_, err = sqlConnection.Exec(query)
	if err != nil {
		return
	}
	return
}
