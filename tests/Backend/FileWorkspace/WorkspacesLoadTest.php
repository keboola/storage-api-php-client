<?php

namespace Keboola\Test\Backend\FileWorkspace;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\StorageApi\ClientException;
use Keboola\Test\Backend\FileWorkspace\Backend\Abs;
use Keboola\Test\Backend\Workspaces\Backend\InputMappingConverter;

class WorkspacesLoadTest extends FileWorkspaceTestCase
{
    public function testWorkspaceLoadData(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

        //setup test tables
        $table1Csv = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($table1Csv)
        );

        $table2Csv = __DIR__ . '/../../_data/numbers.csv';
        $table2Id = $this->_client->createTableAsync(
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
        // upload file again to get new fileId
        $file2Id = $this->_client->uploadFile(
            (new CsvFile($file1Csv))->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['test-file-1'])
        );

        $mapping1 = [
            'source' => $table1Id,
            'destination' => 'tableLanguagesLoaded',
        ];
        $mapping2 = [
            'source' => $table2Id,
            'destination' => 'tableNumbersLoaded',
        ];
        $mapping3 = [
            'dataFileId' => $fileId,
            'destination' => 'fileLanguagesLoaded',
        ];

        $input = [$mapping1, $mapping2, $mapping3];

        // test if job is created and listed
        $initialJobs = $this->_client->listJobs();
        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => $input]);
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

        $this->assertManifest($backend, 'tableLanguagesLoaded');
        $this->assertManifest($backend, 'tableNumbersLoaded');

        $data = $backend->fetchAll('tableLanguagesLoaded', ['id', 'name'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );
        $data = $backend->fetchAll('tableNumbersLoaded', ['0','1','2','3','45'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table2Csv),
            $data,
            0
        );
        $data = $backend->fetchAll('fileLanguagesLoaded', ['id', 'name'], true, true, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($file1Csv),
            $data,
            0
        );

        $blobs = $backend->listFiles('fileLanguagesLoaded/');
        $this->assertCount(1, $blobs); // one file upload in folder
        // load table again with second file into same destination with preserve
        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'dataFileId' => $file2Id,
                    'destination' => 'fileLanguagesLoaded',
                ],
            ],
            'preserve' => true,
        ]);
        $blobs = $backend->listFiles('fileLanguagesLoaded/');
        $this->assertCount(2, $blobs); // two file uploads in folder

        // load table again to new destination to test if workspace was cleared
        $workspaces->loadWorkspaceData($workspace['id'], [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'tableLoadAgain',
                ],
                [
                    'dataFileId' => $file2Id,
                    'destination' => 'fileLanguagesLoaded2',
                ],
            ],
        ]);
        $blobs = $backend->listFiles('tableLanguagesLoaded/');
        $this->assertCount(0, $blobs);
        $blobs = $backend->listFiles('tableNumbersLoaded/');
        $this->assertCount(0, $blobs);
        $blobs = $backend->listFiles('fileLanguagesLoaded/');
        $this->assertCount(0, $blobs);
        $this->assertManifest($backend, 'tableLoadAgain');

        try {
            // load table again to same destination with preserve
            $workspaces->loadWorkspaceData($workspace['id'], [
                'input' => [
                    [
                        'source' => $table1Id,
                        'destination' => 'tableLoadAgain',
                    ],
                ],
                'preserve' => true,
            ]);
            $this->fail('Loading table to same destination must throw exception.');
        } catch (ClientException $e) {
            $this->assertEquals(
                'Table tableLoadAgain already exists in workspace',
                $e->getMessage()
            );
        }

        try {
            // load file upload again to same destination with preserve
            $workspaces->loadWorkspaceData($workspace['id'], [
                'input' => [
                    [
                        'dataFileId' => $file2Id,
                        'destination' => 'fileLanguagesLoaded2',
                    ],
                ],
                'preserve' => true,
            ]);
            $this->fail('Loading same file to same destination must throw exception.');
        } catch (ClientException $e) {
            $this->assertEquals(
                "File \"fileLanguagesLoaded2/{$file2Id}\" already exists in workspace",
                $e->getMessage()
            );
        }
    }

    public function testWorkspaceLoadFilePermissionsCanReadAllFiles(): void
    {
        $fileCsv = __DIR__ . '/../../_data/languages.more-rows.csv';
        $fileId = $this->_client->uploadFile(
            (new CsvFile($fileCsv))->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['test-file-1'])
        );

        // non admin token having canReadAllFileUploads permission
        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Files test')
            ->setCanReadAllFileUploads(true)
        ;

        $newToken = $this->tokens->createToken($tokenOptions);
        $newTokenClient = $this->getClient([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $workspaces = new Workspaces($newTokenClient);
        $workspace = $this->createFileWorkspace($workspaces);

        $mapping = [
            'dataFileId' => $fileId,
            'destination' => 'languagesLoadedMore',
        ];

        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);

        $backend = new Abs($workspace['connection']);
        $data = $backend->fetchAll('languagesLoadedMore', ['id', 'name'], true, true, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($fileCsv),
            $data,
            0
        );

        // non admin token without canReadAllFileUploads permission
        $this->tokens->updateToken(
            (new TokenUpdateOptions($newToken['id']))
                ->setCanReadAllFileUploads(false)
        );

        try {
            $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping]]);
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
            $this->assertSame('accessDenied', $e->getStringCode());
        }
    }

    public function testWorkspaceLoadAliasTable(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

        //setup test tables
        $table1Csv = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTableAsync(
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

        $mapping1 = ['source' => $table1Id, 'destination' => 'languagesLoaded'];
        $mapping2 = ['source' => $table2Id, 'destination' => 'languagesAlias'];
        $mapping3 = ['source' => $table3Id, 'destination' => 'languagesOneColumn'];
        $mapping4 = ['source' => $table4Id, 'destination' => 'languagesFiltered'];
        $mapping5 = ['source' => $table2AliasedId, 'destination' => 'languagesNestedAlias'];

        $input = [$mapping1, $mapping2, $mapping3, $mapping4, $mapping5];
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => $input]);

        $backend = new Abs($workspace['connection']);

        $this->assertManifest($backend, 'languagesLoaded');
        $this->assertManifest($backend, 'languagesAlias');
        $this->assertManifest($backend, 'languagesOneColumn');
        $this->assertManifest($backend, 'languagesFiltered');
        $this->assertManifest($backend, 'languagesNestedAlias');

        $data = $backend->fetchAll('languagesLoaded', ['id', 'name'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );
        $data = $backend->fetchAll('languagesAlias', ['id', 'name'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );
        $this->assertEquals(1, $backend->countRows('languagesFiltered', ['id', 'name'], false));

        $data = $backend->fetchAll('languagesOneColumn', ['id'], false, false);
        foreach ($data as $row) {
            $this->assertCount(1, $row);
        }
    }

    public function testWorkspaceLoadColumns(): void
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

        $backend = new Abs($workspace['connection']);

        //setup test tables
        $tableId = $this->_client->createTableAsync(
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
            $this->fail('Trying to select a non existent column should fail');
        } catch (ClientException $e) {
            $this->assertEquals('storage.tables.nonExistingColumns', $e->getStringCode());
        }
    }

    public function testRowsParameter(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);
        $backend = new Abs($workspace['connection']);

        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
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
    public function testWorkspaceExportFilters($exportOptions, $expectedResult): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'users', new CsvFile($importFile));

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

        $data = $backend->fetchAll('filter-test', ['id', 'name', 'city', 'sex'], false, true);

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
                        '1',
                        'martin',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'female',
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
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
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
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
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
                        '5',
                        'hidden',
                        '',
                        'male',
                    ],
                    [
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
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
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '5',
                        'hidden',
                        '',
                        'male',
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
                        '5',
                        'hidden',
                        '',
                        'male',
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
                        '4',
                        'miro',
                        'BRA',
                        'male',
                    ],
                    [
                        '1',
                        'martin',
                        'PRG',
                        'male',
                    ],
                    [
                        '2',
                        'klara',
                        'PRG',
                        'female',
                    ],
                    [
                        '3',
                        'ondra',
                        'VAN',
                        'male',
                    ],
                ],
            ],
        ];
    }

    public function testDuplicateDestination(): void
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

        //setup test tables
        $table1_id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $table2_id = $this->_client->createTableAsync(
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

    public function testSourceTableNotFound(): void
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

    public function testDataFileIdNotFound(): void
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

        // let's try loading from a table that doesn't exist
        $mappingInvalidSource = [
            'dataFileId' => 'this-is-for-sure-not-file',
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
            $this->assertEquals('workspace.dataFileIdNotFound', $e->getStringCode());
        }
    }

    public function testInvalidInputs(): void
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $this->createFileWorkspace($workspaces);

        //setup test tables
        $table1_id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        //  test invalid destination double /
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $table1_id,
                            'destination' => 'languages//Loaded',
                        ],
                    ],
                ]
            );
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }

        //  test invalid destination special characters
        try {
            $workspaces->loadWorkspaceData(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $table1_id,
                            'destination' => 'languages*(&#$@(Loaded',
                        ],
                    ],
                ]
            );
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }

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

        $input = [$mapping1];
        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            ['input' => $mapping1]
        );
        //  test for non-array input
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('input should be an array of mappings.');
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
        unset($testMapping['destination']);
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
        unset($testMapping['source']);
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

    public function testInvalidBucketPermissions(): void
    {
        // make a test table
        $tableId = $this->_client->createTableAsync(
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
        $workspace = $this->createFileWorkspace($workspaces);

        $input = [
            [
                'source' => $tableId,
                'destination' => 'irrelevant',
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

    public function testDottedDestination(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

        // Create a table of sample data
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages_dotted',
            new CsvFile($importFile)
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'dotted.destination',
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

    public function testLoadWorkspaceWithOverwrite(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $this->createFileWorkspace($workspaces);

        $table1Csv = __DIR__ . '/../../_data/languages.csv';
        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages',
            new CsvFile($table1Csv)
        );

        $table2Csv = __DIR__ . '/../../_data/users.csv';
        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'users',
            new CsvFile($table2Csv)
        );

        $backend = new Abs($workspace['connection']);

        $file1Csv = __DIR__ . '/../../_data/languages.csv';
        $file1Id = $this->_client->uploadFile(
            (new CsvFile($file1Csv))->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['test-file-1'])
        );

        $file2Csv = __DIR__ . '/../../_data/users.csv';
        $file2Id = $this->_client->uploadFile(
            (new CsvFile($file2Csv))->getPathname(),
            (new FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(true)
                ->setTags(['test-file-2'])
        );

        $options = [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'tableLanguages',
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'tableUsers',
                ],
                [
                    'dataFileId' => $file1Id,
                    'destination' => 'fileLanguages',
                ],
                [
                    'dataFileId' => $file2Id,
                    'destination' => 'fileUsers',
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $data = $backend->fetchAll('tableLanguages', ['id','name'], false, false);
        $this->assertCount(6, $data);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table1Csv),
            $data,
            0
        );

        $data = $backend->fetchAll('tableUsers', ['id','name', 'city', 'sex'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table2Csv),
            $data,
            0
        );

        $data = $backend->fetchAll('fileLanguages', ['id', 'name'], true, true, false);
        $this->assertCount(6, $data);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($file1Csv),
            $data,
            0
        );

        $data = $backend->fetchAll('fileUsers', ['id','name', 'city', 'sex'], true, true, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($file2Csv),
            $data,
            0
        );

        $table3Csv = __DIR__ . '/../../_data/languages.more-rows-no-duplicates.csv';
        $table3Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languagesMoreRows',
            new CsvFile($table3Csv)
        );

        $options = [
            'preserve' => true,
            'input' => [
                [
                    'source' => $table3Id,
                    'destination' => 'tableLanguages',
                    'overwrite' => true,
                ],
                [
                    'dataFileId' => $file1Id,
                    'destination' => 'fileLanguages',
                ],
            ],
        ];

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            $this->fail('Loading same file to same destination must throw exception.');
        } catch (ClientException $e) {
            $this->assertEquals(
                "File \"fileLanguages/{$file1Id}\" already exists in workspace",
                $e->getMessage()
            );
        }

        $options = [
            'preserve' => true,
            'input' => [
                [
                    'source' => $table3Id,
                    'destination' => 'tableLanguages',
                    'overwrite' => true,
                ],
                [
                    'dataFileId' => $file1Id,
                    'destination' => 'fileLanguages',
                    'overwrite' => true,
                ],
            ],
        ];

        // load should overwrite existing tables with new values
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // test load from table overwrite existing data instead of add new data
        $data = $backend->fetchAll('tableLanguages', ['id', 'name'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table3Csv),
            $data,
            0
        );

        // test load from file overwrite existing data instead of add new data
        $data = $backend->fetchAll('fileLanguages', ['id', 'name'], true, true, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($file1Csv),
            $data,
            0
        );

        // test table created from table before overwrite should be preserved
        $data = $backend->fetchAll('tableUsers', ['id','name', 'city', 'sex'], false, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($table2Csv),
            $data,
            0
        );

        // test table created from file before overwrite table should be preserved
        $data = $backend->fetchAll('fileUsers', ['id','name', 'city', 'sex'], true, true, false);
        $this->assertArrayEqualsSorted(
            $this->_readCsv($file2Csv),
            $data,
            0
        );
    }
}
