<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Csv\CsvFile;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class LegacyWorkspacesRedshiftTest extends ParallelWorkspacesTestCase
{

    /**
     * @dataProvider columnCompressionDataTypesDefinitions
     * @param $dataTypesDefinition
     */
    public function testColumnCompression($dataTypesDefinition): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages-rs',
            new CsvFile($importFile),
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages-rs',
                    'datatypes' => $dataTypesDefinition,
                ],
            ],
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $table = $backend->describeTableColumns('languages-rs');

        $this->assertEquals('varchar', $table['id']['DATA_TYPE']);
        $this->assertEquals(50, $table['id']['LENGTH']);
        $this->assertEquals('lzo', $table['id']['COMPRESSION']);

        $this->assertEquals('varchar', $table['name']['DATA_TYPE']);
        $this->assertEquals(255, $table['name']['LENGTH']);
        $this->assertEquals('bytedict', $table['name']['COMPRESSION']);
    }

    public function columnCompressionDataTypesDefinitions()
    {
        return [
            [
                [
                    'id' => 'VARCHAR(50)',
                    'name' => 'VARCHAR(255) ENCODE BYTEDICT',
                ],
            ],
            [
                [
                    [
                        'column' => 'id',
                        'type' => 'VARCHAR',
                        'length' => '50',
                    ],
                    [
                        'column' => 'name',
                        'type' => 'VARCHAR',
                        'length' => '255',
                        'compression' => 'BYTEDICT',
                    ],
                ],
            ],
        ];
    }

    public function testLoadedPrimaryKeys(): void
    {
        $primaries = ['Paid_Search_Engine_Account', 'Date', 'Paid_Search_Campaign', 'Paid_Search_Ad_ID', 'Site__DFA'];
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

        // there are no columns new input mapping is used, thus PK length will be 65535
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);

        $cols = $backend->describeTableColumns('languages-pk');
        $this->assertCount(6, $cols);
        $this->assertEquals('varchar', $cols['paid_search_engine_account']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['paid_search_engine_account']['LENGTH']);
        $this->assertEquals('varchar', $cols['date']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['date']['LENGTH']);
        $this->assertEquals('varchar', $cols['paid_search_campaign']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['paid_search_campaign']['LENGTH']);
        $this->assertEquals('varchar', $cols['paid_search_ad_id']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['paid_search_ad_id']['LENGTH']);
        $this->assertEquals('varchar', $cols['site__dfa']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['site__dfa']['LENGTH']);
        $this->assertEquals('varchar', $cols['advertiser_id']['DATA_TYPE']);
        $this->assertEquals(65535, $cols['advertiser_id']['LENGTH']);

        // Check that PK is NOT set if not all PK columns are present
        $mapping2 = [
            'source' => $pkTableId,
            'destination' => 'languages-pk-skipped',
            'columns' => ['Paid_Search_Engine_Account', 'Date'], // missing PK columns
        ];
        // there are columns as array of strings, legacy input mapping is used, thus PK length will be 255
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping2]]);

        $cols = $backend->describeTableColumns('languages-pk-skipped');
        $this->assertCount(2, $cols);
        $this->assertEquals('varchar', $cols['paid_search_engine_account']['DATA_TYPE']);
        $this->assertEquals(255, $cols['paid_search_engine_account']['LENGTH']);
        $this->assertEquals('varchar', $cols['date']['DATA_TYPE']);
        $this->assertEquals(255, $cols['date']['LENGTH']);
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
            $this->assertArrayHasKey('state', $row);
            $this->assertArrayHasKey('id', $row);

            if (in_array($row['id'], ['0', '11', '24'])) {
                $this->assertNull($row['state']);
            }
        }
    }
}
