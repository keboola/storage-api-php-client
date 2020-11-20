<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\FileWorkspace\Backend\Abs;
use Keboola\Test\Backend\Workspaces\Backend\InputMappingConverter;

class WorkspacesLoadTest extends FileWorkspaceTestCase
{
    public function testWorkspaceLoadData()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

        //setup test tables
        $table1Csv = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($table1Csv)
        );

        $table2Csv = __DIR__ . '/../../_data/numbers.csv';
        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'numbers',
            new CsvFile($table2Csv)
        );

        $file1Csv = __DIR__ . '/../../_data/languages.more-rows.csv';
        $fileId = $this->_client->uploadFile(
            (new CsvFile($file1Csv))->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['test-file-1'])
        );

        $mapping1 = [
            "source" => $table1Id,
            "destination" => "languagesLoaded",
        ];
        $mapping2 = [
            "source" => $table2Id,
            "destination" => "numbersLoaded",
        ];
        $mapping3 = [
            "dataFileId" => $fileId,
            "destination" => "languagesLoadedMore",
        ];

        $input = [$mapping1, $mapping2, $mapping3];

        // test if job is created and listed
        $initialJobs = $this->_client->listJobs();
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $workspaces->loadWorkspaceData($workspace['id'], ["input" => $input]);
        $afterJobs = $this->_client->listJobs();

        $this->assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        $this->assertNotEquals($initialJobs[0]['id'], $afterJobs[0]['id']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        $this->assertEquals(2, $export['totalCount']);
        $this->assertCount(2, $export['tables']);

        $backend = new Abs($workspace['connection']);

        $this->assertManifest($backend, 'languagesLoaded');
        $this->assertManifest($backend, 'numbersLoaded');

        $data = $backend->fetchAll('languagesLoaded', ["id", "name"], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );
        $data = $backend->fetchAll('numbersLoaded', ["0","1","2","3","45"], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table2Csv),
            $data,
            0
        );
        $data = $backend->fetchAll('languagesLoadedMore', ["id", "name"], true, true, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($file1Csv),
            $data,
            0
        );
        // load table again to new destination to test if workspace was cleared
        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "second",
                ],
            ],
        ]);
        $blobs = $backend->listFiles('languagesLoaded');
        $this->assertCount(0, $blobs);
        $blobs = $backend->listFiles('numbersLoaded');
        $this->assertCount(0, $blobs);
        $blobs = $backend->listFiles('languagesLoadedMore');
        $this->assertCount(0, $blobs);
        $this->assertManifest($backend, 'second');

        // load table again to same destination with preserve
        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Table second already exists in workspace');
        $workspaces->loadWorkspaceData($workspace['id'], [
            "input" => [
                [
                    "source" => $table1Id,
                    "destination" => "second",
                ],
            ],
            'preserve' => true,
        ]);
    }

    public function testWorkspaceLoadAliasTable()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

        //setup test tables
        $table1Csv = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($table1Csv),
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

        $backend = new Abs($workspace['connection']);

        $this->assertManifest($backend, 'languagesLoaded');
        $this->assertManifest($backend, 'languagesAlias');
        $this->assertManifest($backend, 'languagesOneColumn');
        $this->assertManifest($backend, 'languagesFiltered');
        $this->assertManifest($backend, 'languagesNestedAlias');

        $data = $backend->fetchAll('languagesLoaded', ["id", "name"], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );
        $data = $backend->fetchAll('languagesAlias', ["id", "name"], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );
        $this->assertEquals(1, $backend->countRows('languagesFiltered', ["id", "name"], false));

        $data = $backend->fetchAll('languagesOneColumn', ["id"], false, false);
        foreach ($data as $row) {
            $this->assertCount(1, $row);
        }
    }

    public function testWorkspaceLoadColumns()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

        $backend = new Abs($workspace['connection']);

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

        $this->assertManifest($backend, 'languagesIso');
        $this->assertManifest($backend, 'languagesSomething');

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

    public function testRowsParameter()
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);
        $backend = new Abs($workspace['connection']);

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

        $numRows = $backend->countRows('languages', ['id', 'name'], false);
        $this->assertEquals(2, $numRows, 'rows parameter');
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
        $workspace = $this->createFileWorkspace($workspaces);
        $backend = new Abs($workspace['connection']);

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

        $data = $backend->fetchAll('filter-test', ["id", "name", "city", "sex"], false, true);

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

    public function testDuplicateDestination()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

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

    public function testSourceTableNotFound()
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

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
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

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

        $tokenId = $this->_client->createToken($tokenOptions);
        $token = $this->_client->getToken($tokenId);

        $testClient = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        // create the workspace with the limited permission client
        $workspaces = new Workspaces($testClient);
        $workspace = $this->createFileWorkspace($workspaces);

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
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

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

        $backend = new Abs($workspace['connection']);
        $this->assertManifest($backend, 'dotted.destination');
    }

    /**
     * @param Abs $backend
     * @param string $destination
     */
    private function assertManifest(Abs $backend, $destination)
    {
        $files = $backend->listFiles($destination);
        $manifestExists = false;
        foreach ($files as $file) {
            if (strpos($file->getName(), 'manifest') !== false) {
                $manifestExists = true;
            }
        }
        $this->assertTrue($manifestExists);
    }
}
