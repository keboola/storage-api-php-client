<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Utils\PemKeyCertificateGenerator;
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
        $key = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $workspace = $this->initTestWorkspace(
            options: [
                'backend' => self::BACKEND_SNOWFLAKE,
                'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
                'publicKey' => $key->getPublicKey(),
            ],
            forceRecreate: true,
        );
        $workspaceId = $workspace['id'];
        $workspace['connection']['privateKey'] = $key->getPrivateKey();
        $backend = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);

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

        $table = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'test',
            new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $testDropDatabase = $workspaces->executeQuery(
            $workspaceId,
            sprintf('DROP DATABASE %s', $workspace['connection']['database']),
        );
        $this->assertSame(
            $testDropDatabase,
            [
                'status' => 'error',
                'message' => sprintf("An exception occurred while executing a query: SQL access control error:
Insufficient privileges to operate on database '%s'.", $workspace['connection']['database']),
            ],
        );

        $testDropSchema = $workspaces->executeQuery(
            $workspaceId,
            sprintf('DROP SCHEMA %s', $workspace['connection']['schema']),
        );
        $this->assertSame(
            $testDropSchema,
            [
                'status' => 'error',
                'message' => sprintf("An exception occurred while executing a query: SQL access control error:
Insufficient privileges to operate on schema '%s'.", $workspace['connection']['schema']),
            ],
        );

        // in most cases project role should be same as project database
        $testUseProjectUserRole = $workspaces->executeQuery(
            $workspaceId,
            sprintf('USE ROLE %s', $workspace['connection']['database']),
        );
        $this->assertSame(
            $testUseProjectUserRole,
            [
                'status' => 'error',
                'message' => 'An exception occurred while executing a query: SQL execution error:
Current session is restricted. USE ROLE not allowed.',
            ],
        );

        $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'test-delete',
            new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );

        $testDropTable = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'DROP TABLE %s.%s."test-delete"',
                $workspace['connection']['database'],
                SnowflakeQuote::quoteSingleIdentifier($this->getTestBucketId(self::STAGE_IN)),
            ),
        );
        $this->assertSame(
            $testDropTable,
            [
                'status' => 'error',
                'message' => "An exception occurred while executing a query: SQL access control error:
Insufficient privileges to operate on table 'test-delete'.",
            ],
        );

        // in most cases project role should be same as project database
        $executeImmediate = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                <<<EOD
EXECUTE IMMEDIATE $$
BEGIN
  USE ROLE %s;
  DROP TABLE %s.%s."test-delete";
  RETURN 'done';
END;
$$
;
EOD,
                $workspace['connection']['database'],
                $workspace['connection']['database'],
                SnowflakeQuote::quoteSingleIdentifier($this->getTestBucketId(self::STAGE_IN)),
            ),
        );
        $this->assertSame(
            $executeImmediate,
            [
                'status' => 'error',
                'message' => "An exception occurred while executing a query: Uncaught exception of type 'STATEMENT_ERROR' on line 3 at position 2 : SQL execution error:
Current session is restricted. USE ROLE not allowed.",
            ],
        );
    }

    public function testWorkspaceQueryLegacyService(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $branchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $workspaces = new Workspaces($branchClient);
        $workspace = $this->initTestWorkspace(
            options: [
                'backend' => self::BACKEND_SNOWFLAKE,
                'loginType' => WorkspaceLoginType::SNOWFLAKE_LEGACY_SERVICE_PASSWORD,
            ],
            forceRecreate: true,
        );
        try {
            $workspaces->executeQuery(
                $workspace['id'],
                sprintf(
                    'CREATE OR REPLACE TABLE %s (ID INT, NAME VARCHAR(32))',
                    SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
                ),
            );
            $this->fail('Executing query on workspace with legacy service login type should fail.');
        } catch (ClientException $e) {
            $this->assertSame('storage.executeQuery.notSupportedLoginType', $e->getStringCode());
        }

        $workspace = $this->initTestWorkspace(
            options: [
                'backend' => self::BACKEND_SNOWFLAKE,
                'loginType' => WorkspaceLoginType::DEFAULT,
            ],
            forceRecreate: true,
        );
        try {
            $workspaces->executeQuery(
                $workspace['id'],
                sprintf(
                    'CREATE OR REPLACE TABLE %s (ID INT, NAME VARCHAR(32))',
                    SnowflakeQuote::quoteSingleIdentifier(self::TABLE),
                ),
            );
            $this->fail('Executing query on workspace with legacy service login type should fail.');
        } catch (ClientException $e) {
            $this->assertSame('storage.executeQuery.notSupportedLoginType', $e->getStringCode());
        }
    }
}
