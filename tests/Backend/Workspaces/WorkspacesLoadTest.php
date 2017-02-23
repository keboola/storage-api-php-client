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
                    'columns' => ["Id","iso"]
                ],
                [
                    'source' => $tableId,
                    'destination' => 'languagesSomething',
                    'columns' => ["Name","Something"]
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
                    'columns' => ["Id","iso","not-a-column"]
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
        sleep(15);
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
                    'seconds' => floor(time() - $startTime) + 10,
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
                'rows' => 2
            ]
        ]);

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $numrows = $backend->countRows('languages');
        $this->assertEquals(2, $numrows, 'rows parameter');
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
        $this->_client->markTableColumnAsIndexed($tableId, 'city');

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

    public function testDataTypes()
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
                'datatypes' => [
                    "Id" => "INTEGER",
                    "Name" => "VARCHAR(50)"
                ]
            ]
        ]);

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        //check to make sure the columns have the right types
        $columnInfo = $backend->describeTableColumns('datatype_Test');

        foreach ($columnInfo as $colInfo) {
            switch ($colInfo['name']) {
                case 'id':
                case 'Id':
                    if ($workspace['connection']['backend'] === $this::BACKEND_SNOWFLAKE) {
                        $this->assertEquals("NUMBER(38,0)", $colInfo['type']);
                    }
                    if ($workspace['connection']['backend'] === $this::BACKEND_REDSHIFT) {
                        $this->assertEquals("int4", $colInfo['DATA_TYPE']);
                    }
                    break;
                case 'name':
                case 'Name':
                    if ($workspace['connection']['backend'] === $this::BACKEND_SNOWFLAKE) {
                        $this->assertEquals("VARCHAR(50)", $colInfo['type']);
                    }
                    if ($workspace['connection']['backend'] === $this::BACKEND_REDSHIFT) {
                        $this->assertEquals("varchar", $colInfo['DATA_TYPE']);
                    }
                    break;
                default:
                    $this->fail("Unknown column " . $colInfo['name']);
            }
        }
    }

    public function testDataTypeConversionUserError()
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
                'datatypes' => [
                    "id" => "INTEGER",
                    "name" => "INTEGER"
                ]
            ]
        ]);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.tableLoad', $e->getStringCode());
            $this::assertContains($tableId, $e->getMessage());
        }

        // table should be created but we should be able to delete it
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $this->assertNotEmpty($backend->describeTableColumns('datatype_test'));
        $backend->dropTable('datatype_test');
    }

    public function testDataTypeForNotExistingColumnUserError()
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
                'datatypes' => [
                    "id" => "INTEGER", // lower case instead camel case should be resolved likne non-existing column
                    "Name" => "VARCHAR(50)"
                ]
            ]
        ]);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.datatypesForNonExistingColumns', $e->getStringCode());
        }
    }

    public function testInvalidDataTypeUserError()
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
                'datatypes' => [
                    "id" => "CISLO",
                    "name" => "CISLO"
                ]
            ]
        ]);

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Workspace should not be loaded');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.tableCreate', $e->getStringCode());
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
        $workspaces = new Workspaces($this->_client);
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
        $workspaces = new Workspaces($this->_client);

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
                "destination" => "irrelevant"
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
                ]
            ]
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        // let's try to delete some columns
        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertEquals('dotted.destination', $tables[0]);
    }
}
