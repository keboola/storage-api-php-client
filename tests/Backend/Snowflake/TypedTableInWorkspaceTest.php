<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;

class TypedTableInWorkspaceTest extends ParallelWorkspacesTestCase
{
    private string $tableId;

    public function setUp(): void
    {
        parent::setUp();
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

// todo: return this https://keboola.atlassian.net/browse/CT-1108
//        try {
//            // __temp_ table should be typed
//            $this->_client->writeTableAsyncDirect($this->tableId, [
//                'dataWorkspaceId' => $workspace['id'],
//                'dataTableName' => $tableId,
//            ]);
//            $this->fail('Should fail since createTableDefinition method creates table with VARCHAR type with no length');
//        } catch (ClientException $e) {
//            $this->assertSame('Table import error: Source destination columns mismatch. "id FLOAT NOT NULL"->"id NUMBER (38,0)"', $e->getMessage());
//        }

        $db->query(sprintf('DROP TABLE %s', $quotedTableId));
        // length of VARCHAR is lower than what is in storage but it still works
        $sql = sprintf(
            '
            CREATE TABLE %s (
                "id" INT NOT NULL,
                "name" VARCHAR(1677)
            )
        ',
            $quotedTableId,
        );
        $db->query($sql);
        $db->query(sprintf("INSERT INTO %s VALUES (1, 'john');", $quotedTableId));
        $db->query(sprintf("INSERT INTO %s VALUES (2, 'does');", $quotedTableId));

        $this->unloadAndAssert($workspace['id'], $tableId);

        // test unload from workspace with _timestamp column exist
        $db->query(sprintf('DROP TABLE %s', $quotedTableId));
        $sql = sprintf(
            '
            CREATE TABLE %s (
                "id" INT NOT NULL,
                "name" VARCHAR(16777216),
                "_timestamp" STRING --it actually does not matter type of _timestamp column it is just ignored
            )
        ',
            $quotedTableId,
        );
        $db->query($sql);
        $db->query(sprintf("INSERT INTO %s VALUES (1, 'john', '');", $quotedTableId));
        $db->query(sprintf("INSERT INTO %s VALUES (2, 'does', '');", $quotedTableId));

        $this->unloadAndAssert($workspace['id'], $tableId);
    }

    private function unloadAndAssert(int $id, string $tableId): void
    {
        // should be OK tables types are matching
        $this->_client->writeTableAsyncDirect($this->tableId, [
            'dataWorkspaceId' => $id,
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

    public function testImportObjectToWorkspace(): void
    {
        $bucketId = $this->getTestBucketId();

        $payload = [
            'name' => 'with-empty',
            'primaryKeysNames' => [],
            'columns' => [
                ['name' => 'id', 'definition' => ['type' => 'NUMBER']],
                ['name' => 'col', 'definition' => ['type' => 'OBJECT']],
            ],
        ];
        $tableId = $this->_client->createTableDefinition($bucketId, $payload);

        $workspace = $this->initTestWorkspace(self::BACKEND_SNOWFLAKE);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backend->dropTableIfExists('test_object');

        /** @var Connection $db */
        $db = $backend->getDb();

        $qb = new SnowflakeTableQueryBuilder();
        $query = $qb->getCreateTableCommand(
            $workspace['connection']['schema'],
            'test_object',
            new ColumnCollection([
                new SnowflakeColumn('id', new Snowflake('INT')),
                new SnowflakeColumn('col', new Snowflake('OBJECT')),
            ])
        );
        $db->query($query);
        $backend->executeQuery(sprintf(
        /** @lang Snowflake */
            '
INSERT INTO "%s"."test_object" ("id", "col") select 2, OBJECT_CONSTRUCT(\'name\', \'Jones\'::VARIANT, \'age\',  42::VARIANT);',
            $workspace['connection']['schema']
        ));

        $this->_client->writeTableAsyncDirect($tableId, [
            'dataWorkspaceId' => $workspace['id'],
            'dataTableName' => 'test_object',
        ]);

        $workspaces = new Workspaces($this->_client);
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'test_object_import',
                    ],
                ],
            ]
        );

        $backend->fetchAll('SELECT * FROM "test_object_import"');

    }

}

