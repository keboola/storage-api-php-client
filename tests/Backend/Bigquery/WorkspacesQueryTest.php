<?php

namespace Keboola\Test\Backend\Bigquery;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\WorkspaceCredentialsAssertTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use PDO;

class WorkspacesQueryTest extends ParallelWorkspacesTestCase
{
    private const TABLE = 'CREW';
    private const NON_EXISTING_WORKSPACE_ID = 2147483647;
    use WorkspaceConnectionTrait;
    use WorkspaceCredentialsAssertTrait;

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
        try {
            $workspaces->executeQuery(
                self::NON_EXISTING_WORKSPACE_ID,
                'SELECT * FROM DOES_NOT_MATTER',
            );
            $this->fail('Executing query on non-existing workspace should fail.');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Workspace "%d" not found.', self::NON_EXISTING_WORKSPACE_ID),
                $e->getMessage(),
            );
        }

        // Create new table
        $this->assertEmpty($backend->getTables());
        $createTable = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'CREATE OR REPLACE TABLE %s (ID INT64, NAME STRING)',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );

        $this->assertStringContainsString('executed successfully', $createTable['message']);
        $this->assertSame('ok', $createTable['status']);
        $this->assertCount(1, $backend->getTables());

        // Insert data
        $insert = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                "INSERT INTO %s VALUES (13, 'Scoop'), (42, 'Bob The Builder')",
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertStringContainsString('executed successfully', $insert['message']);
        $this->assertSame('ok', $insert['status']);

        // Select data
        $select = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'SELECT * FROM %s',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame('ok', $select['status']);
        $this->assertStringContainsString('executed successfully', $select['message']);
        $this->assertCount(2, $select['data']['rows']);

        $this->assertEqualsCanonicalizing([
            0 => [
                'ID' => '13',
                'NAME' => 'Scoop',
            ],
            1 => [
                'ID' => '42',
                'NAME' => 'Bob The Builder',
            ],
        ], $select['data']['rows']);

        // Select empty
        $selectEmpty = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'SELECT * FROM %s WHERE ID = 666',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );

        $this->assertStringContainsString('executed successfully', $selectEmpty['message']);
        $this->assertSame('ok', $selectEmpty['status']);
        $this->assertEmpty($selectEmpty['data']['rows']);

        // Alter table
        $alterTable = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'ALTER TABLE %s ADD COLUMN IS_VEHICLE BOOLEAN;',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertStringContainsString('executed successfully', $alterTable['message']);
        $this->assertSame('ok', $alterTable['status']);

        $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'UPDATE %s SET IS_VEHICLE = TRUE WHERE 1=1',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );

        $cols = $backend->getTableColumns(self::TABLE);
        $this->assertSame(['ID', 'NAME', 'IS_VEHICLE'], $cols);

//         Update data
        $update = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'UPDATE %s SET IS_VEHICLE = TRUE WHERE ID = 13',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertStringContainsString('executed successfully', $update['message']);
        $this->assertSame('ok', $update['status']);

        $updated = $backend->fetchAll(self::TABLE, PDO::FETCH_ASSOC, 'ID');
        $this->assertSame(
            [
                'ID' => '13',
                'NAME' => 'Scoop',
                'IS_VEHICLE' => '1',
            ],
            $updated[0],
        );

//         Delete data
        $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'DELETE FROM %s WHERE ID = 42',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertSame(1, $backend->countRows(self::TABLE));

        // CTAS (CREATE TABLE AS SELECT)
        $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                'CREATE TABLE CREW_COPY AS SELECT * FROM %s',
                BigqueryQuote::quoteSingleIdentifier(self::TABLE),
            ),
        );
        $this->assertCount(2, $backend->getTables());

        // CTE (Common Table Expression)
        $withSelect = $workspaces->executeQuery(
            $workspaceId,
            sprintf(
                "WITH CONCAT AS (SELECT CONCAT(ID, '-' ,NAME) as SLUG FROM `%s`) SELECT * FROM CONCAT",
                self::TABLE,
            ),
        );
        $this->assertSame($withSelect['data']['rows'][0]['SLUG'], '13-Scoop');

        // Error
        $errorSQL = $workspaces->executeQuery(
            $workspaceId,
            'SELECT * FROM BLACK_HOLE',
        );

        $this->assertStringContainsString('BLACK_HOLE was not found', $errorSQL['message']);
        $this->assertSame('error', $errorSQL['status']);
    }
}
