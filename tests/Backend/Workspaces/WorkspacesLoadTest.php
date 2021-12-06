<?php

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\Test\Backend\Workspaces\Backend\InputMappingConverter;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspacesLoadTest extends ParallelWorkspacesTestCase
{
    public function testWorkspaceTablesPermissions()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();

        //setup test tables
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'INTEGER',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'VARCHAR',
                        ],
                    ],
                ],
                [
                    'source' => $tableId,
                    'destination' => 'langs',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'INTEGER',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'VARCHAR',
                        ],
                    ],
                ],
            ],
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // let's try to delete some columns
        $backend->dropTableColumn('languages', 'id');

        $backend->dropTable('languages');

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertEquals('langs', $tables[0]);
    }

    public function testWorkspaceLoadData()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $mapping1 = ["source" => $table1_id, "destination" => "languagesLoaded"];
        $mapping2 = ["source" => $table2_id, "destination" => "numbersLoaded"];

        $input = [$mapping1, $mapping2];

        // test if job is created and listed
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspaces->loadWorkspaceData($workspace['id'], ["input" => $input]);

        $afterJobs = $this->listWorkspaceJobs($workspace['id']);
        $lastJob = reset($afterJobs);
        $this->assertEquals('workspaceLoad', $lastJob['operationName']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        $this->assertEquals(2, $export['totalCount']);
        $this->assertCount(2, $export['tables']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(2, $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);

        // check table structure and data
        $data = $backend->fetchAll("languagesLoaded", \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');

        // now we'll load another table and use the preserve parameters to check that all tables are present
        $mapping3 = ["source" => $table1_id, "destination" => "table3"];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping3], "preserve" => true]);

        $tables = $backend->getTables();

        $this->assertCount(3, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => [$mapping3]]);

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
    }

    public function testWorkspaceLoadAliasTable()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $this->initTestWorkspace();

        //setup test tables
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $table2Id = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $table1Id,
            'Languages'
        );

        // nested alias
        $table2AliasedId = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $table2Id,
            'LanguagesNestedAlias'
        );

        $table3Id = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $table1Id,
            'LanguagesOneColumn',
            [
                'aliasColumns' => [
                    'id',
                ],
            ]
        );

        $table4Id = $this->_client->createAliasTable(
            $this->getTestBucketId(self::STAGE_OUT),
            $table1Id,
            'LanguagesFiltered',
            [
                'aliasColumns' => [
                    'id',
                ],
                'aliasFilter' => [
                    'column' => 'id',
                    'values' => ['1'],
                ],
            ]
        );

        $mapping1 = ["source" => $table1Id, "destination" => "languagesLoaded"];
        $mapping2 = ["source" => $table2Id, "destination" => "languagesAlias"];
        $mapping3 = ["source" => $table3Id, "destination" => "languagesOneColumn"];
        $mapping4 = ["source" => $table4Id, "destination" => "languagesFiltered"];
        $mapping5 = ["source" => $table2AliasedId, "destination" => "languagesNestedAlias"];

        $input = [$mapping1, $mapping2, $mapping3, $mapping4, $mapping5];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => $input]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(5, $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("languagesAlias"), $tables);
        $this->assertContains($backend->toIdentifier("languagesOneColumn"), $tables);
        $this->assertContains($backend->toIdentifier("languagesFiltered"), $tables);
        $this->assertContains($backend->toIdentifier("languagesNestedAlias"), $tables);

        // check table structure and data
        // first table
        $data = $backend->fetchAll("languagesLoaded", \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');

        // second table
        $data = $backend->fetchAll("languagesAlias", \PDO::FETCH_ASSOC);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');

        // third table
        $data = $backend->fetchAll("languagesOneColumn", \PDO::FETCH_ASSOC);

        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);
        $expected = Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"');
        $expected = array_map(function ($row) {
            return [
                'id' => $row['id'],
            ];
        }, $expected);
        $this->assertArrayEqualsSorted($expected, $data, 'id');

        // fourth table
        $data = $backend->fetchAll("languagesFiltered", \PDO::FETCH_ASSOC);
        $this->assertCount(1, $data[0], 'there should be one column');
        $this->assertArrayHasKey('id', $data[0]);

        $this->assertEquals('1', $data[0]['id']);

        // fifth table
        $data = $backend->fetchAll("languagesNestedAlias", \PDO::FETCH_ASSOC);
        $this->assertArrayEqualsSorted(Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'), $data, 'id');
    }

    public function testWorkspaceLoadColumns()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        //setup test tables
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languagesColumns',
            new CsvFile(__DIR__ . '/../../_data/languages-more-columns.csv')
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesIso',
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'iso',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languagesSomething',
                    'columns' => [
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'Something',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];

        $mappingColumns = [
            array_map(
                function ($row) {
                    return $row['source'];
                },
                $options['input'][0]['columns']
            ),
            array_map(
                function ($row) {
                    return $row['source'];
                },
                $options['input'][1]['columns']
            ),
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // check that the tables have the appropriate columns
        $columns = $backend->getTableColumns($backend->toIdentifier("languagesIso"));
        $this->assertEquals(2, count($columns));
        $this->assertEquals(0, count(array_diff($columns, $backend->toIdentifier($mappingColumns[0]))));

        $columns = $backend->getTableColumns($backend->toIdentifier("languagesSomething"));
        $this->assertEquals(2, count($columns));
        $this->assertEquals(0, count(array_diff($columns, $backend->toIdentifier($mappingColumns[1]))));

        // test for invalid columns
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesIso',
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'iso',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'not-a-column',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail("Trying to select a non existent column should fail");
        } catch (ClientException $e) {
            $this->assertEquals("storage.tables.nonExistingColumns", $e->getStringCode());
        }
    }

    public function testLoadIncrementalWithColumns()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id']
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['dd', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );

        $workspaces->loadWorkspaceData($workspace['id'], $options);
//        $this->assertEquals(2, $backend->countRows("languagesDetails"));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));
    }

    public function testIncrementalAdditionalColumns()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languages"));

        $this->_client->addTableColumn($tableId, 'test');

        // second load with additional columns
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'test',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.columnsNotMatch', $e->getStringCode());
            $this->assertContains('columns are missing in workspace table', $e->getMessage());
            $this->assertContains('languages', $e->getMessage());
        }
    }

    public function testIncrementalMissingColumns()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languages"));

        $this->_client->deleteTableColumn($tableId, 'name');

        // second load with additional columns
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languages',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.columnsNotMatch', $e->getStringCode());
            $this->assertContains('columns are missing in source table', $e->getMessage());
            $this->assertContains($tableId, $e->getMessage());
        }
    }

    /**
     * @dataProvider columnsErrorDefinitions
     */
    public function testIncrementalDataTypesDiff($table, $firstLoadDataColumns, $secondLoadDataColumns)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . "/../../_data/$table.csv";

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            $table,
            new CsvFile($importFile)
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => $table,
                    'columns' => $firstLoadDataColumns,
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // second load - incremental
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => $table,
                    'columns' => $secondLoadDataColumns,
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Incremental load with different datatypes should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.columnsTypesNotMatch', $e->getStringCode());
            $this->assertContains('Different mapping between', $e->getMessage());
        }
    }

    public function testSecondsFilter()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );
        $originalFileLinesCount = exec("wc -l <" . escapeshellarg($importFile));
        sleep(35);
        $startTime = time();
        $importCsv = new \Keboola\Csv\CsvFile($importFile);
        $this->_client->writeTable($tableId, $importCsv, [
            'incremental' => true,
        ]);
        $this->_client->writeTable($tableId, $importCsv, [
            'incremental' => true,
        ]);

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'seconds' => floor(time() - $startTime) + 30,
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        // ok, the table should only have rows from the 2 most recent loads
        $numRows = $backend->countRows("languages");
        $this->assertEquals(2 * ($originalFileLinesCount - 1), $numRows, "seconds parameter");
    }

    public function testRowsParameter()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'rows' => 2,
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $numrows = $backend->countRows('languages');
        $this->assertEquals(2, $numrows, 'rows parameter');
    }

    /**
     * @param $exportOptions
     * @param $expectedResult
     * @dataProvider workspaceExportFiltersData
     */
    public function testWorkspaceExportFilters($exportOptions, $expectedResult)
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $options = [
            'input' => [
                array_merge([
                    'source' => $tableId,
                    'destination' => 'filter-test',
                ], $exportOptions),
            ],
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $data = $backend->fetchAll('filter-test');

        $this->assertArrayEqualsSorted($expectedResult, $data, 0);
    }

    public function workspaceExportFiltersData()
    {
        return [
            // first test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "1",
                        "martin",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "female",
                    ],
                ],
            ],
            // first test with defined operator
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'whereOperator' => 'eq',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "1",
                        "martin",
                        "PRG",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ],
                ],
            ],
            // second test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'VAN'],
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "1",
                        "martin",
                        "PRG",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ],
                    [
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ],
                ],
            ],
            // third test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG'],
                    'whereOperator' => 'ne',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "5",
                        "hidden",
                        "",
                        "male",
                    ],
                    [
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ],
                    [
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ],
                ],
            ],
            // fourth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => ['PRG', 'VAN'],
                    'whereOperator' => 'ne',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ],
                    [
                        "5",
                        "hidden",
                        "",
                        "male",
                    ],
                ],
            ],
            // fifth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => [''],
                    'whereOperator' => 'eq',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "5",
                        "hidden",
                        "",
                        "male",
                    ],
                ],
            ],
            // sixth test
            [
                [
                    'whereColumn' => 'city',
                    'whereValues' => [''],
                    'whereOperator' => 'ne',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'city',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'sex',
                            'type' => 'varchar',
                        ],
                    ],
                ],
                [
                    [
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ],
                    [
                        "1",
                        "martin",
                        "PRG",
                        "male",
                    ],
                    [
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ],
                    [
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider validColumnsDefinitions
     * @param $columnsDefinition
     */
    public function testDataTypes($columnsDefinition, $expectedColumns)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'datatype_Test',
                    'columns' => $columnsDefinition,
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        //check to make sure the columns have the right types
        $columnInfo = $backend->describeTableColumns($backend->toIdentifier('datatype_Test'));
        $expectedColumnsForBackend = $expectedColumns[$workspace['connection']['backend']];
        $this->assertCount(count($expectedColumnsForBackend), $columnInfo);
        if ($columnInfo instanceof ColumnCollection) {
            $columnsAsArray = [];
            /** @var ColumnInterface $item */
            foreach ($columnInfo as $item) {
                $columnsAsArray[$item->getColumnName()] = $item->getColumnDefinition()->toArray();
            }
            $columnInfo = $columnsAsArray;
        }
        /** @var array $item */
        foreach ($columnInfo as &$item) {
            if (array_key_exists('SCHEMA_NAME', $item)) {
                unset($item['SCHEMA_NAME']);
            }
            if (array_key_exists('policy name', $item)) {
                unset($item['policy name']);
            }
        }
        unset($item);
        $this->assertSame(
            $expectedColumns[$workspace['connection']['backend']],
            $columnInfo
        );
    }

    /**
     * @dataProvider conversionUserErrorColumnsDefinitions
     * @param $columnsDefinition
     */
    public function testDataTypeConversionUserError($columnsDefinition)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'datatype_test',
                    'columns' => $columnsDefinition,
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.tableLoad', $e->getStringCode());
            $this->assertContains($tableId, $e->getMessage());
        }

        // table should be created but we should be able to delete it
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $this->assertNotEmpty($backend->describeTableColumns('datatype_test'));
        $backend->dropTable('datatype_test');
    }

    /**
     * @dataProvider notExistingColumnUserErrorColumnsDefinitions
     * @param $columnsDefinition
     */
    public function testDataTypeForNotExistingColumnUserError($columnsDefinition)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'datatype_Test',
                    'columns' => $columnsDefinition,
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.nonExistingColumns', $e->getStringCode());
        }
    }

    public function testInvalidExtendedColumnUserError()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'datatype_test',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'UNKNOWN',
                        ],
                        [
                            'source' => 'name',
                            'type' => 'UNKNOWN',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.inputMapping', $e->getStringCode());
        }
    }

    public function testDuplicateDestination()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $table2_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        // now let's try and load 2 different sources to the same destination, this request should be rejected
        $mapping1 = [
            'source' => $table1_id,
            'destination' => 'languagesLoaded',
            'columns' => [
                [
                    'source' => 'id',
                    'type' => 'INTEGER',
                ],
                [
                    'source' => 'name',
                    'type' => 'VARCHAR',
                ],
            ],
        ];
        $mapping2 = [
            'source' => $table2_id,
            'destination' => 'languagesLoaded',
            'columns' => [
                [
                    'source' => '0',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => '1',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => '2',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => '3',
                    'type' => 'VARCHAR',
                ],
                [
                    'source' => '5',
                    'type' => 'VARCHAR',
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => [$mapping1, $mapping2]]
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Attempt to write two sources to same destination should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateDestination', $e->getStringCode());
        }
    }

    public function testTableAlreadyExistsAndOverwrite()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'Languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $secondTableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'Languages2',
            new CsvFile(__DIR__ . '/../../_data/languages.more-rows.csv')
        );
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                        'columns' => [
                            [
                                'source' => 'id',
                                'type' => 'INTEGER',
                            ],
                            [
                                'source' => 'name',
                                'type' => 'VARCHAR',
                            ],
                        ],
                    ],
                ],
            ]
        );
        // first load
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            $options
        );
        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(5, $workspaceTableData);

        // load of same table with preserve
        $options['preserve'] = true;
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                $options
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateTable', $e->getStringCode());
        }

        // load with overwrite and incremental
        $options['input'][0]['overwrite'] = true;
        $options['input'][0]['incremental'] = true;
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                $options
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestLogicalException', $e->getStringCode());
        }

        // load with overwrite and not preserved
        $options['preserve'] = false;
        $options['input'][0]['incremental'] = false;
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                $options
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestLogicalException', $e->getStringCode());
        }

        // load with overwrite and preserve, second table with more rows
        $options['preserve'] = true;
        $options['input'][0]['overwrite'] = true;
        $options['input'][0]['source'] = $secondTableId;
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            $options
        );
        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(7, $workspaceTableData);
    }

    public function testSourceTableNotFound()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        // let's try loading from a table that doesn't exist
        $mappingInvalidSource = [
            'source' => 'in.c-nonExistentBucket.fakeTable',
            'destination' => 'whatever',
            'columns' => [
                [
                    'source' => 'fake',
                    'type' => 'fake',
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => [$mappingInvalidSource]]
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Source does not exist, this should fail');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('workspace.sourceNotFound', $e->getStringCode());
        }
    }

    public function testInvalidInputs()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $mapping1 = [
            "source" => $table1_id,
            "destination" => "languagesLoaded",
            "columns" => [
                [
                    "source" => "id",
                    "type" => "INTEGER",
                ],
                [
                    "source" => "name",
                    "type" => "VARCHAR",
                ],
            ],
        ];
        $input = [$mapping1];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => $mapping1]
        );
        //  test for non-array input
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail("input should be an array of mappings.");
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => $input]
        );
        // test for invalid workspace id
        try {
            $workspaces->loadWorkspaceData(0, $options);
            $this->fail('Should not be able to find a workspace with id 0');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('workspace.workspaceNotFound', $e->getStringCode());
        }

        // test invalid input parameter
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $input);
            $this->fail('Should return bad request, input is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestInputRequired', $e->getStringCode());
        }

        $testMapping = $mapping1;
        unset($testMapping["destination"]);
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => [$testMapping]]
        );

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Should return bad request, destination is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }

        $testMapping = $mapping1;
        unset($testMapping["source"]);
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => [$testMapping]]
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Should return bad request, source is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }
    }

    public function testInvalidBucketPermissions()
    {
        // make a test table
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('workspaceLoadTest: Out read token')
            ->addBucketPermission($this->getTestBucketId(self::STAGE_OUT), TokenAbstractOptions::BUCKET_PERMISSION_READ);

        $token = $this->tokens->createToken($tokenOptions);

        $testClient = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        // create the workspace with the limited permission client
        $workspaces = new Workspaces($testClient);
        $workspace = $workspaces->createWorkspace();

        $input = [
            [
                "source" => $tableId,
                "destination" => "irrelevant",
                "columns" => [
                    [
                        "source" => "id",
                        "type" => "INTEGER",
                    ],
                    [
                        "source" => "name",
                        "type" => "VARCHAR",
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => $input]
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('This should fail due to insufficient permission');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('workspace.tableAccessDenied', $e->getStringCode());
        }
    }

    public function testDottedDestination()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages_dotted',
            new CsvFile($importFile)
        );

        $options = [
            "input" => [
                [
                    "source" => $tableId,
                    "destination" => "dotted.destination",
                    "columns" => [
                        [
                            "source" => "id",
                            "type" => "INTEGER",
                        ],
                        [
                            "source" => "name",
                            "type" => "VARCHAR",
                        ],
                    ],
                ],
            ],
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        // let's try to delete some columns
        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertEquals('dotted.destination', $tables[0]);
    }

    public function testLoadIncrementalWithColumnsReorder()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages-more-columns.csv';
        $tableId = $this->_client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFile),
            ['primaryKey' => 'Id']
        );

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['dd', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(2, $backend->countRows("languagesDetails"));

        // second load
        $options = [
            'input' => [
                [
                    'incremental' => true,
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                    'whereColumn' => 'iso',
                    'whereValues' => ['ff', 'xx'],
                    'columns' => [
                        [
                            'source' => 'Name',
                            'type' => 'varchar',
                        ],
                        [
                            'source' => 'Id',
                            'type' => 'integer',
                        ],
                    ],
                ],
            ],
        ];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));
    }

    public function validColumnsDefinitions()
    {
        return [
            'full column definition' => [
                'columnsDefinition' => [
                    [
                        'source' => 'Id',
                        'type' => 'INTEGER',
                    ],
                    [
                        'source' => 'Name',
                        'type' => 'VARCHAR',
                        'length' => '50',
                    ],
                ],
                'expectedColumns' => [
                    self::BACKEND_SNOWFLAKE => [
                        [
                            'name' => 'Id',
                            'type' => 'NUMBER(38,0)',
                            'kind' => 'COLUMN',
                            'null?' => 'Y',
                            'default' => null,
                            'primary key' => 'N',
                            'unique key' => 'N',
                            'check' => null,
                            'expression' => null,
                            'comment' => null,
                        ],
                        [
                            'name' => 'Name',
                            'type' => 'VARCHAR(50)',
                            'kind' => 'COLUMN',
                            'null?' => 'Y',
                            'default' => null,
                            'primary key' => 'N',
                            'unique key' => 'N',
                            'check' => null,
                            'expression' => null,
                            'comment' => null,
                        ],
                    ],
                    self::BACKEND_SYNAPSE => [
                        'Id' => [
                            'type' => 'INT',
                            'length' => null,
                            'nullable' => true,
                        ],
                        'Name' => [
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => true,
                        ],
                    ],
                    self::BACKEND_REDSHIFT => [
                        'id' => [
                            'TABLE_NAME' => 'datatype_test',
                            'name' => 'id',
                            'COLUMN_POSITION' => 1,
                            'DATA_TYPE' => 'int4',
                            'DEFAULT' => null,
                            'NULLABLE' => true,
                            'LENGTH' => 4,
                            'SCALE' => null,
                            'PRECISION' => null,
                            'UNSIGNED' => null,
                            'PRIMARY' => false,
                            'PRIMARY_POSITION' => null,
                            'IDENTITY' => false,
                            'COMPRESSION' => 'az64',
                        ],
                        'name' => [
                            'TABLE_NAME' => 'datatype_test',
                            'name' => 'name',
                            'COLUMN_POSITION' => 2,
                            'DATA_TYPE' => 'varchar',
                            'DEFAULT' => null,
                            'NULLABLE' => true,
                            'LENGTH' => '50',
                            'SCALE' => null,
                            'PRECISION' => null,
                            'UNSIGNED' => null,
                            'PRIMARY' => false,
                            'PRIMARY_POSITION' => null,
                            'IDENTITY' => false,
                            'COMPRESSION' => 'lzo',
                        ],
                    ],
                    self::BACKEND_EXASOL => [
                        'Id' => [
                            'type' => 'DECIMAL',
                            'length' => '3,0',
                            'nullable' => true,
                        ],
                        'Name' => [
                            'type' => 'VARCHAR',
                            'length' => '50',
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
            'without type' => [
                'columnsDefinition' => [
                    [
                        'source' => 'Id',
                    ],
                    [
                        'source' => 'Name',
                    ],
                ],
                'expectedColumns' => [
                    self::BACKEND_SNOWFLAKE => [
                        [
                            'name' => 'Id',
                            'type' => 'VARCHAR(16777216)',
                            'kind' => 'COLUMN',
                            'null?' => 'Y',
                            'default' => null,
                            'primary key' => 'N',
                            'unique key' => 'N',
                            'check' => null,
                            'expression' => null,
                            'comment' => null,
                        ],
                        [
                            'name' => 'Name',
                            'type' => 'VARCHAR(16777216)',
                            'kind' => 'COLUMN',
                            'null?' => 'Y',
                            'default' => null,
                            'primary key' => 'N',
                            'unique key' => 'N',
                            'check' => null,
                            'expression' => null,
                            'comment' => null,
                        ],
                    ],
                    self::BACKEND_SYNAPSE => [
                        'Id' => [
                            'type' => 'NVARCHAR',
                            'length' => '4000',
                            'nullable' => true,
                        ],
                        'Name' => [
                            'type' => 'NVARCHAR',
                            'length' => '4000',
                            'nullable' => true,
                        ],
                    ],
                    self::BACKEND_REDSHIFT => [
                        'id' => [
                            'TABLE_NAME' => 'datatype_test',
                            'name' => 'id',
                            'COLUMN_POSITION' => 1,
                            'DATA_TYPE' => 'varchar',
                            'DEFAULT' => null,
                            'NULLABLE' => true,
                            'LENGTH' => '65535',
                            'SCALE' => null,
                            'PRECISION' => null,
                            'UNSIGNED' => null,
                            'PRIMARY' => false,
                            'PRIMARY_POSITION' => null,
                            'IDENTITY' => false,
                            'COMPRESSION' => 'lzo',
                        ],
                        'name' => [
                            'TABLE_NAME' => 'datatype_test',
                            'name' => 'name',
                            'COLUMN_POSITION' => 2,
                            'DATA_TYPE' => 'varchar',
                            'DEFAULT' => null,
                            'NULLABLE' => true,
                            'LENGTH' => '65535',
                            'SCALE' => null,
                            'PRECISION' => null,
                            'UNSIGNED' => null,
                            'PRIMARY' => false,
                            'PRIMARY_POSITION' => null,
                            'IDENTITY' => false,
                            'COMPRESSION' => 'lzo',
                        ],
                    ],
                    self::BACKEND_EXASOL => [
                        'Id' => [
                            'type' => 'VARCHAR',
                            'length' => '2000000',
                            'nullable' => true,
                        ],
                        'Name' => [
                            'type' => 'VARCHAR',
                            'length' => '2000000',
                            'nullable' => true,
                        ],
                    ],
                ],
            ],
        ];
    }

    public function conversionUserErrorColumnsDefinitions()
    {
        return [
            [
                [
                    [
                        'source' => 'id',
                        'type' => 'INTEGER',
                    ],
                    [
                        'source' => 'name',
                        'type' => 'INTEGER',
                    ],
                ],
            ],
        ];
    }

    public function notExistingColumnUserErrorColumnsDefinitions()
    {
        return [
            [
                [
                    'source' => 'id', // lower case instead camel case should be resolved like non-existing column
                    'type' => 'INTEGER',
                ],
                [
                    'source' => 'Name',
                    'type' => 'VARCHAR',
                    'length' => '50',
                ],
            ],
        ];
    }

    public function columnsErrorDefinitions()
    {
        return [
            [
                'languages',
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'convertEmptyValuesToNull' => false,
                    ],
                ],
                [
                    [
                        'source' => 'name',
                        'type' => 'CHARACTER',
                        'convertEmptyValuesToNull' => false,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 30,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                    ],
                ],
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 30,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => false,
                    ],
                ],
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => false,
                    ],
                ],
                [
                    [
                        'source' => 'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => true,
                    ],
                ],
            ],
        ];
    }

    public function testLoadWithWrongInput()
    {
        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace();

        try {
            $workspacesClient->loadWorkspaceData($workspace['id'], [
                'input' => 'this is not array',
            ]);
            $this->fail('Test should not reach this line');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
            $this->assertEquals(
                'Argument "input" is expected to be type "array", value "this is not array" given.',
                $e->getMessage()
            );
        }
    }

    /**
     * @return void
     */
    public function testCreateWorkspaceWithReadOnlyIM()
    {
        $token = $this->_client->verifyToken();

        if (!in_array('input-mapping-read-only-storage', $token['owner']['features'])) {
            $this->markTestSkipped(sprintf('Read only mapping is not enabled for project "%s"', $token['owner']['id']));
        }

        // prepare bucket
        $testBucketId = $this->getTestBucketId();
        $testBucketName = str_replace('in.c-', '', $testBucketId);

        // prepare table in the bucket
        $this->_client->createTable(
            $testBucketId,
            'animals',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // prepare workspace
        $workspace = $this->initTestWorkspace();

        if ($workspace['connection']['backend'] !== 'snowflake') {
            $this->fail('This feature works only for Snowflake at the moment');
        }

        // prepare table in the bucket created after workspace created
        $this->_client->createTable(
            $testBucketId,
            'trains',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        /** @var SnowflakeWorkspaceBackend $backend */
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $db = $backend->getDb();

        $projectDatabase = $workspace['connection']['database'];
        $quotedProjectDatabase = $db->quoteIdentifier($projectDatabase);
        $quotedTestBucketId = $db->quoteIdentifier($testBucketId);

        $db->query(sprintf(
            'CREATE TABLE "tableFromAnimals" AS SELECT * FROM %s.%s."animals"',
            $quotedProjectDatabase,
            $quotedTestBucketId
        ));
        $this->assertCount(5, $db->fetchAll('SELECT * FROM "tableFromAnimals"'));

        $db->query(sprintf(
            'CREATE TABLE "tableFromTrains" AS SELECT * FROM %s.%s."trains"',
            $quotedProjectDatabase,
            $quotedTestBucketId
        ));
        $this->assertCount(5, $db->fetchAll('SELECT * FROM "tableFromTrains"'));
    }
}
