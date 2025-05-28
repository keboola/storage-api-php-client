<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use PDO;

class WorkspacesQueryTest extends ParallelWorkspacesTestCase
{
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

    private const TABLE = 'CREW';
    private const NON_EXISTING_WORKSPACE_ID = 2147483647;

    public function testWorkspaceQuery(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $workspaces = new Workspaces($branchClient);
        $workspace = $this->initTestWorkspace(
            forceRecreate: true,
        );
        $workspaceId = $workspace['id'];
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // Workspace not found
        $exception = null;
        try {
            $workspaces->executeQuery(self::NON_EXISTING_WORKSPACE_ID, 'SHOW TABLES');
            $this->fail('Executing query on non-existing workspace should fail.');
        } catch (ClientException $e) {
            $exception = $e;
        }

        $this->assertInstanceOf(ClientException::class, $exception);
        $this->assertSame(
            sprintf('Workspace "%d" not found.', self::NON_EXISTING_WORKSPACE_ID),
            $exception->getMessage(),
        );

        // Create new table
        $this->assertEmpty($backend->getTables());
        $createTable = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'CREATE OR REPLACE TABLE %s (ID INT, NAME VARCHAR(32))',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(
            $createTable,
            [
                'status' => 'ok',
                'message' => 'Statement executed successfully.',
            ],
        );
        $this->assertCount(1, $backend->getTables());

        // Insert data
        $insert = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                "INSERT INTO %s VALUES (13, 'Scoop'), (42, 'Bob The Builder')",
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(
            $insert,
            [
                'status' => 'ok',
                'message' => '2 rows affected.',
            ],
        );

        // Select data
        $select = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                '    SELECT * FROM %s      ',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(
            $select,
            [
                'status' => 'ok',
                'data' => [
                    'columns' => [
                        'ID',
                        'NAME',
                    ],
                    'rows' => [
                        0 => [
                            'ID' => '13',
                            'NAME' => 'Scoop',
                        ],
                        1 => [
                            'ID' => '42',
                            'NAME' => 'Bob The Builder',
                        ],
                    ],
                ],
            ],
        );

        // Select empty
        $selectEmpty = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'SELECT * FROM %s WHERE ID = 666',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(
            $selectEmpty,
            [
                'status' => 'ok',
                'data' => [
                    'columns' => [],
                    'rows' => [],
                ],
            ],
        );

        // Alter table
        $alterTable = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'ALTER TABLE %s ADD COLUMN IS_VEHICLE BOOLEAN DEFAULT FALSE;',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(
            $alterTable,
            [
                'status' => 'ok',
                'message' => 'Statement executed successfully.',
            ],
        );
        $cols = $backend->getTableColumns(self::TABLE);
        $this->assertSame(['ID', 'NAME', 'IS_VEHICLE'], $cols);

        // Update data
        $update = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'UPDATE %s SET IS_VEHICLE = TRUE WHERE ID = 13',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(
            $update,
            [
                'status' => 'ok',
                'message' => '1 rows affected.',
            ],
        );
        $updated = $backend->fetchAll(self::TABLE, PDO::FETCH_ASSOC, 'ID');
        $this->assertSame(
            [
                'ID' => '13',
                'NAME' => 'Scoop',
                'IS_VEHICLE' => '1',
            ],
            $updated[0],
        );

        // Delete data
        $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'DELETE FROM %s WHERE ID = 42',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(1, $backend->countRows(self::TABLE));

        // CTAS (CREATE TABLE AS SELECT)
        $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'CREATE TABLE CREW_COPY AS SELECT * FROM %s',
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertCount(2, $backend->getTables());

        // CTE (Common Table Expression)
        $withSelect = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                "WITH CONCAT AS (SELECT CONCAT(ID, '-' ,NAME) as SLUG FROM %s) SELECT * FROM CONCAT",
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame($withSelect['data']['rows'][0]['SLUG'], '13-Scoop');

        // Error
        $update = $workspaces->executeQuery(
            $workspaceId,
            'SELECT * FROM BLACK_HOLE',
        );
        $this->assertSame(
            $update,
            [
                'status' => 'error',
                'message' => "An exception occurred while executing a query: SQL compilation error:\nObject 'BLACK_HOLE' does not exist or not authorized.",
            ],
        );
    }
}
