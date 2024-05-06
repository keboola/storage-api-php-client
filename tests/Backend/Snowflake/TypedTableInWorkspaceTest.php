<?php

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
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
            $tableId,
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

    public function testCopyLoadOfTypedTable(): void
    {
        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($workspace);

        $backend->dropTableIfExists('Langs');
        $workspacesClient->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $this->tableId,
                    'destination' => 'Langs',
                ],
            ],
            'preserve' => true,
        ]);

        $columns = $backend->getTableReflection('Langs')->getTableDefinition()->getColumnsDefinitions();

        self::assertCount(2, $columns);
        $table = $this->workspaceSapiClient->getTable($this->tableId);
        $this->assertTableColumns($table['definition']['columns'], $columns);
    }

    private function assertTableColumns(array $columnDefinitionResponse, ColumnCollection $collection): void
    {
        self::assertSame(
            array_map(fn(array $column) => $column['name'], $columnDefinitionResponse),
            array_map(fn(ColumnInterface $column) => $column->getColumnName(), iterator_to_array($collection)),
        );
        /** @var ColumnInterface $column */
        foreach ($collection as $column) {
            $columnDefinition = array_shift($columnDefinitionResponse);
            self::assertEquals($columnDefinition['name'], $column->getColumnName());
            $definition = $column->getColumnDefinition();
            assert($definition instanceof Snowflake);
            self::assertEquals($columnDefinition['definition']['type'], $definition->getType());
            self::assertEquals($columnDefinition['definition']['nullable'], $definition->isNullable());
            self::assertEquals($columnDefinition['definition']['length'], $definition->getLength());
        }
    }
}
