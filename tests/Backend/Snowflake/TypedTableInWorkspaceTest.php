<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class TypedTableInWorkspaceTest extends ParallelWorkspacesTestCase
{
    private string $tableId;

    public function setUp(): void
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();

        $this->tableId = $this->createTableDefinition();
    }

    private function createTableDefinition(): string
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my_new_table',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                        'nullable' => false,
                    ],
                ],
                [
                    'name' => 'name',
                    'definition' => [
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition($bucketId, $data);
    }

    public function testUnloadFromWSToTypedTable(): void
    {
        // create workspace and source table in workspace
        $workspace = $this->initTestWorkspace();

        $tableId = 'Languages3';

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists($tableId);

        $connection = $workspace['connection'];

        $db = $backend->getDb();

        $quotedTableId = sprintf(
            '"%s"."%s"',
            $connection['schema'],
            $tableId
        );

        $sql = sprintf(
            '
            CREATE TABLE %s (
                "id" FLOAT NOT NULL,
                "name" VARCHAR(5000)
            )
        ',
            $quotedTableId,
        );
        $db->query($sql);
        $db->query(sprintf("INSERT INTO %s VALUES (1, 'john');", $quotedTableId));
        $db->query(sprintf("INSERT INTO %s VALUES (2, 'does');", $quotedTableId));

        try {
            // __temp_ table should be typed
            $this->_client->writeTableAsyncDirect($this->tableId, [
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => $tableId,
            ]);
            $this->fail('Should fail since createTableDefinition method creates table with VARCHAR type with no length');
        } catch (ClientException $e) {
            $this->assertSame('Table import error: Source destination columns mismatch. "id FLOAT NOT NULL"->"id NUMBER (38,0)"', $e->getMessage());
        }

        $db->query(sprintf('DROP TABLE %s', $quotedTableId));
        $sql = sprintf(
            '
            CREATE TABLE %s (
                "id" INT NOT NULL,
                "name" VARCHAR(16777216)
            )
        ',
            $quotedTableId,
        );
        $db->query($sql);
        $db->query(sprintf("INSERT INTO %s VALUES (1, 'john');", $quotedTableId));
        $db->query(sprintf("INSERT INTO %s VALUES (2, 'does');", $quotedTableId));

        // should be OK tables types are matching
        $this->_client->writeTableAsyncDirect($this->tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => $tableId,
        ]);

        $expected = [
            [
                [
                    'columnName' => 'id',
                    'value' => '1',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'name',
                    'value' => 'john',
                    'isTruncated' => false,
                ],
            ],
            [
                [
                    'columnName' => 'id',
                    'value' => '2',
                    'isTruncated' => false,
                ],
                [
                    'columnName' => 'name',
                    'value' => 'does',
                    'isTruncated' => false,
                ],
            ],
        ];

        /** @var array $data */
        $data = $this->_client->getTableDataPreview($this->tableId, [
            'format' => 'json',
        ]);
        self::assertEquals($expected, $data['rows']);
    }
}
