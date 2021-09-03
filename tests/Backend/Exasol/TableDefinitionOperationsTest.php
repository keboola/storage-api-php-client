<?php

namespace Keboola\Test\Backend\Exasol;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class TableDefinitionOperationsTest extends StorageApiTestCase
{
    private $tableId;

    public function setUp()
    {
        parent::setUp();

        $token = $this->_client->verifyToken();

        if (!in_array('tables-definition', $token['owner']['features'])) {
            $this->fail(sprintf('Tables definition feature is not enabled for project "%s"', $token['owner']['id']));
        }

        $this->initEmptyTestBucketsForParallelTests();

        $this->tableId = $this->createTableDefinition();
    }

    private function createTableDefinition()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $data = [
            'name' => 'my-new-table',
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
                        'type' => 'NVARCHAR',
                    ],
                ],
            ]
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition($bucketId, $data);
    }

    public function testDataPreviewForTableDefinitionWithNumericTypes()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $tableDefinition = [
            'name' => 'my-new-table',
            'primaryKeysNames' => ['id'],
            'columns' => [
                [
                    'name' => 'id',
                    'definition' => [
                        'type' => 'INT',
                    ],
                ],
                [
                    'name' => 'column_decimal',
                    'definition' => [
                        'type' => 'DECIMAL',
                        'length' => '4,3'
                    ],
                ],
            ],
        ];

        $csvFile = new CsvFile(tempnam(sys_get_temp_dir(), 'keboola'));
        $csvFile->writeRow(['id', 'column_decimal']);
        $csvFile->writeRow(['1', '003.123']);
        $csvFile->writeRow(['3', '4.321']);


        $tableId = $this->_client->createTableDefinition($bucketId, $tableDefinition);

        $this->_client->writeTable($tableId, $csvFile);

        $data = $this->_client->getTableDataPreview($tableId, ['format' => 'json']);

        $this->assertSame(
            [
                [
                    [
                        'columnName' => 'id',
                        'value' => '1',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'column_decimal',
                        'value' => '3.123',
                        'isTruncated' => false,
                    ]
                ],
                [
                    [
                        'columnName' => 'id',
                        'value' => '3',
                        'isTruncated' => false,
                    ],
                    [
                        'columnName' => 'column_decimal',
                        'value' => '4.321',
                        'isTruncated' => false,
                    ]
                ],
            ],
            $data['rows']
        );

        $this->assertSame(2, count($data['rows']));
    }

    public function testAddColumnOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->addTableColumn($this->tableId, 'newColumn');
    }

    public function testDropColumnOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->deleteTableColumn($this->tableId, 'name');
    }

    public function testAddPrimaryKeyOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->createTablePrimaryKey($this->tableId, ['id']);
    }
    public function testRemovePrimaryKeyOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->removeTablePrimaryKey($this->tableId);
    }

    public function testCreateSnapshotOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed tables");
        $this->_client->createTableSnapshot($this->tableId);
    }
}
