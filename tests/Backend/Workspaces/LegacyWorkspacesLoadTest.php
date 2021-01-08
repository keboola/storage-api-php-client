<?php
namespace Keboola\Test\Backend\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\LegacyInputMappingConverter;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class LegacyWorkspacesLoadTest extends ParallelWorkspacesTestCase
{
    public function testWorkspaceTablesPermissions()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);

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
                ],
                [
                    'source' => $tableId,
                    'destination' => 'langs',
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

        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
        $mapping2 = array("source" => $table2_id, "destination" => "numbersLoaded");

        $input = array($mapping1, $mapping2);

        // test if job is created and listed
        $initialJobs = $this->_client->listJobs();
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);
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

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3)));

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);

        // there are two events, dummy (0) and the clone event (1)
        $loadEvent = array_pop($events);

        $this->assertSame('storage.workspaceLoaded', $loadEvent['event']);
        $this->assertSame($runId, $loadEvent['runId']);
        $this->assertSame('storage', $loadEvent['component']);
        $this->assertArrayHasKey('params', $loadEvent);
        $this->assertSame($table1_id, $loadEvent['params']['source']);
        $this->assertSame('table3', $loadEvent['params']['destination']);
        $this->assertArrayHasKey('columns', $loadEvent['params']);
        $this->assertArrayHasKey('workspace', $loadEvent['params']);
    }

    public function dataTypesErrorDefinitions()
    {
        return [
            [
                'languages',
                [
                    'name' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'name',
                        'type' => 'CHARACTER',
                    ],
                ],
            ],
            [
                'languages',
                [
                    'name' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 30,
                    ],
                ],
            ],
            [
                'languages',
                [
                    'name' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 30,
                    ],
                ],
            ],
            [
                'languages',
                [
                    'name' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => false,
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                    ],
                ],
            ],
            [
                'languages',
                [
                    'name' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => false,
                    ],
                ],
                [
                    'id' => [
                        'column' =>  'name',
                        'type' => 'VARCHAR',
                        'length' => 50,
                        'nullable' => true,
                    ],
                ],
            ],
        ];
    }

    public function testWorkspaceLoadColumns()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
                    'columns' => ["Id", "iso"]
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languagesSomething',
                    'columns' => ["Name", "Something"]
                ]
            ]
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // check that the tables have the appropriate columns
        $columns = $backend->getTableColumns($backend->toIdentifier("languagesIso"));
        $this->assertEquals(2, count($columns));
        $this->assertEquals(0, count(array_diff($columns, $backend->toIdentifier($options['input'][0]['columns']))));

        $columns = $backend->getTableColumns($backend->toIdentifier("languagesSomething"));
        $this->assertEquals(2, count($columns));
        $this->assertEquals(0, count(array_diff($columns, $backend->toIdentifier($options['input'][1]['columns']))));

        // test for invalid columns
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesIso',
                    'columns' => ["Id", "iso", "not-a-column"]
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
                    'columns' => ['Id', 'Name'],
                ],
            ],
        ];
//        return;

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
                    'columns' => ['Id', 'Name'],
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $this->assertEquals(5, $backend->countRows("languagesDetails"));
    }

    public function testIncrementalAdditionalColumns()
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

        // first load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
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
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
     * @dataProvider dataTypesErrorDefinitions
     */
    public function testIncrementalDataTypesDiff($table, $firstLoadDataTypes, $secondLoadDataTypes)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $importFile = __DIR__ . "/../../_data/$table.more-columns.csv";
     //   $importFile = __DIR__ . "/../../_data/$table.csv";

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
                    'datatypes' => $firstLoadDataTypes,
                ],
            ],
        ];
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
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
                    'datatypes' => $secondLoadDataTypes,
                ],
            ],
        ];
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
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
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
                'rows' => 2
            ]
        ]);

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $numrows = $backend->countRows('languages');
        $this->assertEquals(2, $numrows, 'rows parameter');
    }

    public function validDataTypesDefinitions()
    {
        return [
            [
                [
                    "Id" => "INTEGER",
                    "Name" => "VARCHAR(50)"
                ]
            ],
            [
                [
                    "Id" => "INTEGER",
                    "Name" => [
                        'column' => 'Name',
                        'type' => 'VARCHAR',
                        'length' => '50'
                    ]
                ]
            ],
            [
                [
                    [
                        'column' => 'Id',
                        'type' => 'INTEGER'
                    ],
                    [
                        'column' => 'Name',
                        'type' => 'VARCHAR',
                        'length' => '50'
                    ]
                ]
            ]
        ];
    }

    /**
     * @dataProvider validDataTypesDefinitions
     * @param $dataTypesDefinition
     */
    public function testDataTypes($dataTypesDefinition)
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

        $options = array('input' => [
            [
                'source' => $tableId,
                'destination' => 'datatype_Test',
                'datatypes' => $dataTypesDefinition
            ]
        ]);
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
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
            $this->assertEquals("varchar", $columnInfo['name']['DATA_TYPE']);
            $this->assertEquals(50, $columnInfo['name']['LENGTH']);
        }
    }


    /**
     * @dataProvider conversionUserErrorDataTypesDefinitions
     * @param $dataTypesDefinition
     */
    public function testDataTypeConversionUserError($dataTypesDefinition)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
                'datatypes' => $dataTypesDefinition
            ]
        ]);
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
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

    public function conversionUserErrorDataTypesDefinitions()
    {
        return [
            [
                [
                    "id" => "INTEGER",
                    "name" => "INTEGER"
                ]
            ],
            [
                [
                    [
                        'column' => 'id',
                        'type' => 'INTEGER'
                    ],
                    [
                        'column' => 'name',
                        'type' => 'INTEGER'
                    ]
                ]
            ]
        ];
    }

    /**
     * @dataProvider notExistingColumnUserErrorDataTypesDefinitions
     * @param $dataTypesDefinition
     */
    public function testDataTypeForNotExistingColumnUserError($dataTypesDefinition)
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
                'datatypes' => [
                    "id" => "INTEGER", // lower case instead camel case should be resolved like non-existing column
                    "Name" => "VARCHAR(50)"
                ]
            ]
        ]);
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.datatypesForNonExistingColumns', $e->getStringCode());
        }
    }

    public function notExistingColumnUserErrorDataTypesDefinitions()
    {
        return [
            [
                "id" => "INTEGER", // lower case instead camel case should be resolved like non-existing column
                "Name" => "VARCHAR(50)"
            ],
            [
                [
                    [
                        'column' => 'id',
                        'type' => 'INTEGER'
                    ],
                    [
                        'column' => 'Name',
                        'type' => 'VARCHAR',
                        'length' => '50'
                    ]
                ]
            ]
        ];
    }

    public function testInvalidDataTypeUserError()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
                'datatypes' =>  [
                    "id" => "UNKNOWN",
                    "name" => "UNKNOWN"
                ]
            ]
        ]);
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options
        );
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.tableCreate', $e->getStringCode());
        }
    }

    public function testInvalidExtendedDataTypeUserError()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
                'datatypes' => [
                    [
                        'column' => 'id',
                        'type' => 'UNKNOWN'
                    ],
                    [
                        'column' => 'name',
                        'type' => 'UNKNOWN'
                    ]
                ]
            ]
        ]);
        $options = LegacyInputMappingConverter::convertInputColumnsTypesForBackend(
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
        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
        $mapping2 = array("source" => $table2_id, "destination" => "languagesLoaded");
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
        $workspaces = new Workspaces($this->workspaceSapiClient);
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
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        // let's try loading from a table that doesn't exist
        $mappingInvalidSource = array("source" => "in.c-nonExistentBucket.fakeTable", "destination" => "whatever");
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
        $workspaces = new Workspaces($this->workspaceSapiClient);

        $workspace = $workspaces->createWorkspace();

        //setup test tables
        $table1_id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $mapping1 = array("source" => $table1_id, "destination" => "languagesLoaded");
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
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => array(array("source" => $table1_id))));
            $this->fail('Should return bad request, destination is required');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }
        try {
            $workspaces->loadWorkspaceData($workspace['id'], array("input" => array(array("destination" => "destination"))));
            $this->fail('Should return bad request, destination is required');
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
            ->addBucketPermission($this->getTestBucketId(self::STAGE_OUT), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $tokenId = $this->_client->createToken($tokenOptions);
        $token = $this->_client->getToken($tokenId);

        $testClient = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL
        ]);

        // create the workspace with the limited permission client
        $workspaces = new Workspaces($testClient);
        $workspace = $workspaces->createWorkspace();

        $input = [
            [
                'source' => $tableId,
                'destination' => 'irrelevant'
            ]
        ];

        try {
            $workspaces->loadWorkspaceData($workspace['id'], ['input' => $input]);
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

        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $tableId,
                    "destination" => "dotted.destination",
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        // let's try to delete some columns
        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertEquals('dotted.destination', $tables[0]);
    }

    public function testInvalidColumnsStringIgnore()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($importFile)
        );

        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $tableId,
                    "destination" => "languages",
                    "columns" => "",
                ],
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        // let's try to delete some columns
        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertEquals('languages', $tables[0]);
    }

    /**
     * @param $exportOptions
     * @param $expectedResult
     * @dataProvider tableExportFiltersData
     */
    public function testWorkspaceExportFilters($exportOptions, $expectedResult)
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));

        $workspaces = new Workspaces($this->workspaceSapiClient);
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
}
