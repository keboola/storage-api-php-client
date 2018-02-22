<?php
/**
 * Created by PhpStorm.
 * User: marc
 * Date: 08/07/2016
 * Time: 15:30
 */

namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class WorkspaceLoadTest extends WorkspacesTestCase
{
    public function testWorkspaceTablesPermissions()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
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
                        ]
                    ]
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
                        ]
                    ]
                ]
            ],
        ]);

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
        $workspaces = new Workspaces($this->_client);

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

        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
        $mapping2 = array("source" => $table2_id, "destination" => "numbersLoaded");

        $input = array($mapping1, $mapping2);

        // test if job is created and listed
        $initialJobs = $this->_client->listJobs();
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));
        $afterJobs = $this->_client->listJobs();


        $this->assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        $this->assertNotEquals($initialJobs[0]['id'], $afterJobs[0]['id']);

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
        $mapping3 = array("source" => $table1_id, "destination" => "table3");
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3), "preserve" => true));

        $tables = $backend->getTables();

        $this->assertCount(3, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3)));

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
    }

    public function testWorkspaceLoadAliasTable()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

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
                    'values' => ['1']
                ]
            ]
        );

        $mapping1 = ["source" => $table1Id, "destination" => "languagesLoaded"];
        $mapping2 = ["source" => $table2Id, "destination" => "languagesAlias"];
        $mapping3 = ["source" => $table3Id, "destination" => "languagesOneColumn"];
        $mapping4 = ["source" => $table4Id, "destination" => "languagesFiltered"];


        $input = [$mapping1, $mapping2, $mapping3, $mapping4];
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => $input]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(4, $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("languagesAlias"), $tables);
        $this->assertContains($backend->toIdentifier("languagesOneColumn"), $tables);
        $this->assertContains($backend->toIdentifier("languagesFiltered"), $tables);

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
    }

    public function testWorkspaceLoadColumns()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

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
                            'type' => 'integer'
                        ],
                        [
                            'source' => 'iso',
                            'type' => 'varchar'
                        ]
                    ]
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languagesSomething',
                    'columns' => [
                        [
                            'source' => 'Name',
                            'type' => 'varchar'
                        ],
                        [
                            'source' => 'Something',
                            'type' => 'varchar'
                        ]
                    ]
                ]
            ]
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
                            'type' => 'varchar'
                        ],
                        [
                            'source' => 'not-a-column',
                            'type' => 'varchar'
                        ]
                    ]
                ]
            ]
        ];

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

        $workspaces = new Workspaces($this->_client);
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

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));
    }

    public function testIncrementalAdditionalColumns()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
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
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
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
        $workspaces = new Workspaces($this->_client);
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
        $workspaces = new Workspaces($this->_client);
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
        $this->_client->writeTable($tableId, $importCsv, array(
            'incremental' => true,
        ));
        $this->_client->writeTable($tableId, $importCsv, array(
            'incremental' => true,
        ));

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

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        // ok, the table should only have rows from the 2 most recent loads
        $numRows = $backend->countRows("languages");
        $this->assertEquals(2 * ($originalFileLinesCount - 1), $numRows, "seconds parameter");
    }

    public function testRowsParameter()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = array('input' => [
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
            ]
        ]);

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

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $options = array(
            "input" => [
                array_merge([
                    "source" => $tableId,
                    "destination" => 'filter-test'
                ], $exportOptions)
            ]
        );

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $data = $backend->fetchAll('filter-test');

        $this->assertArrayEqualsSorted($expectedResult, $data, 0);
    }

    public function workspaceExportFiltersData()
    {
        return array(
            // first test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
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
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "female",
                    ),
                ),
            ),
            // first test with defined operator
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
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
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                ),
            ),
            // second test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'VAN'),
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
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
            // third test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
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
                ),
                array(
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
            // fourth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'VAN'),
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
                ),
                array(
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // fifth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array(''),
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
                ),
                array(
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // sixth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array(''),
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
                ),
                array(
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
        );
    }

    /**
     * @dataProvider validColumnsDefinitions
     * @param $columnsDefinition
     */
    public function testDataTypes($columnsDefinition)
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $importFile = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = array('input' => [
            [
                'source' => $tableId,
                'destination' => 'datatype_Test',
                'columns' => $columnsDefinition
            ]
        ]);

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        //check to make sure the columns have the right types
        $columnInfo = $backend->describeTableColumns($backend->toIdentifier('datatype_Test'));
        $this->assertCount(2, $columnInfo);
        if ($workspace['connection']['backend'] === $this::BACKEND_SNOWFLAKE) {
            $this->assertEquals("Id", $columnInfo[0]['name']);
            $this->assertEquals("NUMBER(38,0)", $columnInfo[0]['type']);
            $this->assertEquals("Name", $columnInfo[1]['name']);
            $this->assertEquals("VARCHAR(50)", $columnInfo[1]['type']);
        }
        if ($workspace['connection']['backend'] === $this::BACKEND_REDSHIFT) {
            $this->assertEquals("int4", $columnInfo['id']['DATA_TYPE']);
            $this->assertEquals("lzo", $columnInfo['id']['COMPRESSION']);
            $this->assertEquals("varchar", $columnInfo['name']['DATA_TYPE']);
            $this->assertEquals(50, $columnInfo['name']['LENGTH']);
            $this->assertEquals("lzo", $columnInfo['name']['COMPRESSION']);
        }
    }

    /**
     * @dataProvider conversionUserErrorColumnsDefinitions
     * @param $columnsDefinition
     */
    public function testDataTypeConversionUserError($columnsDefinition)
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = array('input' => [
            [
                'source' => $tableId,
                'destination' => 'datatype_test',
                'columns' => $columnsDefinition
            ]
        ]);

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
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . '/../../_data/languages.camel-case-columns.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = array('input' => [
            [
                'source' => $tableId,
                'destination' => 'datatype_Test',
                'columns' => $columnsDefinition,
            ]
        ]);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.nonExistingColumns', $e->getStringCode());
        }
    }

    public function testInvalidExtendedColumnUserError()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $options = array('input' => [
            [
                'source' => $tableId,
                'destination' => 'datatype_test',
                'columns' =>  [
                    [
                        'source' => 'id',
                        'type' => 'UNKNOWN',
                    ],
                    [
                        'source' => 'name',
                        'type' => 'UNKNOWN',
                    ]
                ]
            ]
        ]);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.inputMapping', $e->getStringCode());
        }
    }

    public function testDuplicateDestination()
    {
        $workspaces = new Workspaces($this->_client);
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
        $mapping1 = array(
            "source" => $table1_id,
            "destination" => "languagesLoaded",
            "columns" => array(
                array(
                    "source" => "id",
                    "type" => "INTEGER",
                ),
                array(
                    "source" => "name",
                    "type" => "VARCHAR",
                )
            )
        );
        $mapping2 = array(
            "source" => $table2_id,
            "destination" => "languagesLoaded",
            "columns" => array(
                array(
                    "source" => "0",
                    "type" => "VARCHAR",
                ),
                array(
                    "source" => "1",
                    "type" => "VARCHAR",
                ),
                array(
                    "source" => "2",
                    "type" => "VARCHAR",
                ),
                array(
                    "source" => "3",
                    "type" => "VARCHAR",
                ),
                array(
                    "source" => "45",
                    "type" => "VARCHAR",
                )
            )
        );
        $inputDupFail = array($mapping1, $mapping2);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $inputDupFail));
            $this->fail('Attempt to write two sources to same destination should fail');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateDestination', $e->getStringCode());
        }
    }

    public function testTableAlreadyExistsShouldThrowUserError()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'Languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        // first load
        $workspaces->loadWorkspaceData(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                        "columns" => [
                            [
                                "source" => "id",
                                "type" => "INTEGER",
                            ],
                            [
                                "source" => "name",
                                "type" => "VARCHAR",
                            ]
                        ]
                    ]
                ]
            ]
        );

        // second load of same table with preserve
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tableId,
                            'destination' => 'Langs',
                            "columns" => [
                                [
                                    "source" => "id",
                                    "type" => "INTEGER",
                                ],
                                [
                                    "source" => "name",
                                    "type" => "VARCHAR",
                                ]
                            ]
                        ]
                    ],
                    'preserve' => true,
                ]
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateTable', $e->getStringCode());
        }
    }

    public function testSourceTableNotFound()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // let's try loading from a table that doesn't exist
        $mappingInvalidSource = array(
            "source" => "in.c-nonExistentBucket.fakeTable",
            "destination" => "whatever",
            "columns" => array(
                array(
                    "source" => "fake",
                    "type" => "fake"
                )
            )
        );
        $input404 = array($mappingInvalidSource);
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input404));
            $this->fail('Source does not exist, this should fail');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('workspace.sourceNotFound', $e->getStringCode());
        }
    }

    public function testInvalidInputs()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $mapping1 = array(
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
                ]
            ]
        );
        $input = array($mapping1);

        //  test for non-array input
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $mapping1));
            $this->fail("input should be an array of mappings.");
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }

        // test for invalid workspace id
        try {
            $workspaces->loadWorkspaceData(0, array("input" => $input));
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

        try {
            $testMapping = $mapping1;
            unset($testMapping["destination"]);

            $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($testMapping)));
            $this->fail('Should return bad request, destination is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }
        try {
            $testMapping = $mapping1;
            unset($testMapping["source"]);

            $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($testMapping)));
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

        $bucketPermissions = array(
            $this->getTestBucketId(self::STAGE_OUT) => 'read',
        );
        $tokenId = $this->_client->createToken($bucketPermissions, 'workspaceLoadTest: Out read token');
        $token = $this->_client->getToken($tokenId);

        $testClient = new \Keboola\StorageApi\Client(array(
            'token' => $token['token'],
            'url' => STORAGE_API_URL
        ));

        // create the workspace with the limited permission client
        $workspaces = new Workspaces($testClient);
        $workspace = $workspaces->createWorkspace();

        $input = array(
            array(
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
                    ]
                ]
            )
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));
            $this->fail("This should fail due to insufficient permission");
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('workspace.tableAccessDenied', $e->getStringCode());
        }
    }

    public function testDottedDestination()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages_dotted',
            new CsvFile($importFile)
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
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
                        ]
                    ]
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        // let's try to delete some columns
        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertEquals('dotted.destination', $tables[0]);
    }

    public function testLoadIncrementalWithColumnsReorder()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaces = new Workspaces($this->_client);
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
                        ]
                    ],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));
    }

    public function validColumnsDefinitions()
    {
        return [
            [
                [
                    [
                        'source' => 'Id',
                        'type' => 'INTEGER',
                    ],
                    [
                        'source' => 'Name',
                        'type' => 'VARCHAR',
                        'length' => '50',
                    ],
                ]
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
                    ]
                ]
            ]
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
                ]
            ]
        ];
    }

    public function columnsErrorDefinitions()
    {
        return [
            [
                'languages',
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'convertEmptyValuesToNull' => false,
                    ],
                ],
                [
                    [
                        'source' =>  'name',
                        'type' => 'CHARACTER',
                        'convertEmptyValuesToNull' => false,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 30,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                    ],
                ],
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 30,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => false,
                    ],
                ],
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                    ],
                ],
            ],
            [
                'languages',
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => false,
                    ],
                ],
                [
                    [
                        'source' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => true,
                    ],
                ],
            ],
        ];
    }
}
