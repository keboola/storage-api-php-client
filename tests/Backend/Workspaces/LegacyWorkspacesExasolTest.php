<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class LegacyWorkspacesExasolTest extends ParallelWorkspacesTestCase
{

    public function testLoadedPrimaryKeys(): void
    {
        $primaries = ['Paid_Search_Engine_Account','Date','Paid_Search_Campaign','Paid_Search_Ad_ID','Site__DFA'];
        $pkTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-pk',
            new CsvFile(__DIR__ . '/../../_data/multiple-columns-pk.csv'),
            [
                'primaryKey' => implode(',', $primaries),
            ],
        );

        $mapping = [
            'source' => $pkTableId,
            'destination' => 'languages-pk',
        ];

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);

        /** @var ColumnCollection $cols */
        $cols = $backend->describeTableColumns('languages-pk');
        $cols = iterator_to_array($cols->getIterator());
        $this->assertCount(6, $cols);
        $this->assertEquals('Paid_Search_Engine_Account', $cols[0]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $cols[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Advertiser_ID', $cols[1]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $cols[1]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $cols[2]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $cols[2]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Campaign', $cols[3]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $cols[3]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Paid_Search_Ad_ID', $cols[4]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $cols[4]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Site__DFA', $cols[5]->getColumnName());
        $this->assertEquals('VARCHAR (2000000) NOT NULL', $cols[5]->getColumnDefinition()->getSQLDefinition());

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            'source' => $pkTableId,
            'destination' => 'languages-pk-skipped',
            'columns' => ['Paid_Search_Engine_Account','Date'], // missing PK columns
        ];
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping2]]);

        /** @var ColumnCollection $cols */
        $cols = $backend->describeTableColumns('languages-pk-skipped');
        $cols = iterator_to_array($cols->getIterator());
        $this->assertCount(2, $cols);
        $this->assertEquals('Paid_Search_Engine_Account', $cols[0]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $cols[0]->getColumnDefinition()->getSQLDefinition());
        $this->assertEquals('Date', $cols[1]->getColumnName());
        $this->assertEquals('VARCHAR (2000000)', $cols[1]->getColumnDefinition()->getSQLDefinition());
    }

    public function testLoadIncrementalNotNullable(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id'],
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [26, 1],
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(2, $backend->countRows('languages'));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [11, 26, 24],
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => false,
                        ],
                    ],
                ],
            ],
        ];

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Load columns wit NULL should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.tableLoad', $e->getStringCode());
        }
    }

    public function testLoadIncrementalNullable(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.with-state.csv';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
            ['primaryKey' => 'id'],
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [0, 26, 1],
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(3, $backend->countRows('languages'));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'whereColumn' => 'id',
                    'whereValues' => [11, 26, 24],
                    'datatypes' => [
                        'id' => [
                            'column' =>  'id',
                            'type' => 'SMALLINT',
                            'nullable' => false,
                        ],
                        'name' => [
                            'column' =>  'name',
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => false,
                        ],
                        'State' => [
                            'column' =>  'State',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows('languages'));

        $rows = $backend->fetchAll('languages', \PDO::FETCH_ASSOC);
        foreach ($rows as $row) {
            $this->assertArrayHasKey('State', $row);
            $this->assertArrayHasKey('id', $row);

            if (in_array($row['id'], ['0', '11', '24'])) {
                $this->assertNull($row['State']);
            }
        }
    }

    /**
     * @dataProvider dataTypesDiffDefinitions
     */
    public function testsIncrementalDataTypesDiff($table, $firstLoadDataTypes, $secondLoadDataTypes, $shouldFail): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $importFile = __DIR__ . "/../../_data/$table.csv";

        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            $table,
            new CsvFile($importFile),
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => $table,
                    'datatypes' => $firstLoadDataTypes,
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // second load - incremental
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => $table,
                    'datatypes' => $secondLoadDataTypes,
                ],
            ],
        ];

        if ($shouldFail) {
            try {
                $workspaces->loadWorkspaceData($workspace['id'], $options);
                $this->fail('Incremental load with different datatypes should fail');
            } catch (ClientException $e) {
                $this->assertEquals('workspace.columnsTypesNotMatch', $e->getStringCode());
                $this->assertStringContainsString('Different mapping between', $e->getMessage());
            }
        } else {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
        }
    }

    public function dataTypesDiffDefinitions()
    {
        return [
            [
                'rates',
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'TIMESTAMP',
                    ],
                ],
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'TIMESTAMP WITH LOCAL TIME ZONE',
                    ],
                ],
                true,
            ],
            [
                'rates',
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'DATE',
                    ],
                ],
                [
                    'Date' => [
                        'column' =>  'Date',
                        'type' => 'TIMESTAMP',
                    ],
                ],
                true,
            ],
            [
                'languages',
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'SMALLINT',
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'int',
                    ],
                ],
                true,
            ],
            [
                'languages',
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'DECIMAL',
                        'length' => '3,0',
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'id',
                        'type' => 'DOUBLE PRECISION',
                        'length' => '3,0',
                    ],
                ],
                true,
            ],
        ];
    }
}
