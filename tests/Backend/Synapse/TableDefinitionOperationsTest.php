<?php

namespace Keboola\Test\Backend\Synapse;

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
            ],
            'distribution' => [
                'type' => 'HASH',
                'distributionColumnsNames' => ['id'],
            ],
        ];

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        return $this->_client->createTableDefinition($bucketId, $data);
    }

    public function testAddColumnOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed synapse tables");
        $this->_client->addTableColumn($this->tableId, 'newColumn');
    }

    public function testDropColumnOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed synapse tables");
        $this->_client->deleteTableColumn($this->tableId, 'name');
    }

    public function testAddPrimaryKeyOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed synapse tables");
        $this->_client->createTablePrimaryKey($this->tableId, ['id']);
    }
    public function testRemovePrimaryKeyOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed synapse tables");
        $this->_client->removeTablePrimaryKey($this->tableId);
    }

    public function testCreateSnapshotOnTypedTable()
    {
        $this->expectExceptionMessage("Not implemented for typed synapse tables");
        $this->_client->createTableSnapshot($this->tableId);
    }
}
