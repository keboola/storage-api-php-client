<?php



namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class CloneIntoWorkspaceTest extends ParallelWorkspacesTestCase
{
    const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    /**
     * @dataProvider cloneProvider
     * @param int $aliasNestingLevel
     * @throws Exception
     */
    public function testClone($aliasNestingLevel)
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );

        $sourceTableId = $this->createTableAliasChain($sourceTableId, $aliasNestingLevel, 'languages');

        $workspacesClient = new Workspaces($this->workspaceSapiClient);

        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);
        $this->workspaceSapiClient->setRunId($runId);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);

        // test that events are properly created

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));
        $events = $this->_client->listEvents([
            'runId' => $runId,
        ]);
        // there are two events, dummy (0) and the clone event (1)
        $cloneEvent = array_pop($events);
        $this->assertSame('storage.workspaceTableCloned', $cloneEvent['event']);
        $this->assertSame($runId, $cloneEvent['runId']);
        $this->assertSame('storage', $cloneEvent['component']);
        $this->assertSame($sourceTableId, $cloneEvent['objectId']);
        $this->assertArrayHasKey('params', $cloneEvent);
        $this->assertSame($sourceTableId, $cloneEvent['params']['source']);
        $this->assertSame('languagesDetails', $cloneEvent['params']['destination']);
        $this->assertArrayHasKey('workspace', $cloneEvent['params']);

        // test that stats are generated
        $stats = $this->_client->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));
        $this->assertSame(0, $stats['tables']['import']['totalCount']);
        $this->assertSame(1, $stats['tables']['export']['totalCount']);
        $this->assertCount(1, $stats['tables']['export']['tables']);
        $this->assertArrayEqualsIgnoreKeys([
            'id' => $sourceTableId,
            'count' => 1,
        ], $stats['tables']['export']['tables'][0], ['durationTotalSeconds']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $workspaceTableColumns = $backend->describeTableColumns('languagesDetails');
        $this->assertEquals(
            [
                [
                    'name' => 'id',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => 'name',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '_timestamp',
                    'type' => 'TIMESTAMP_NTZ(9)',
                ]
            ],
            array_map(
                function (array $column) {
                    return [
                        'name' => $column['name'],
                        'type' => $column['type'],
                    ];
                },
                $workspaceTableColumns
            )
        );

        $workspaceTableData = $backend->fetchAll('languagesDetails');
        $this->assertCount(5, $workspaceTableData);
    }

    public function cloneProvider()
    {
        return [
          'normal table' => [
              0,
          ],
          'simple alias' => [
              1,
          ],
          'simple alias 2 levels' =>[
              2,
          ],
        ];
    }

    public function testCloneMultipleTables()
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $table1Id = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH)
        );

        $table2Id = $this->_client->createTable(
            $bucketId,
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv')
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
           'input' => [
               [
                   'source' => $table1Id,
                   'destination' => 'languages',
               ],
               [
                   'source' => $table2Id,
                   'destination' => 'rates',
               ]
           ]
        ]);

        $actualJob = null;
        foreach ($this->_client->listJobs() as $job) {
            if ($job['operationName'] === 'workspaceLoadClone') {
                if ((int) $job['operationParams']['workspaceId'] === $workspace['id']) {
                    $actualJob = $job;
                }
            }
        }

        $this->assertNotNull($actualJob);
        $this->assertArrayHasKey('metrics', $actualJob);
        $this->assertEquals(44544, $actualJob['metrics']['outBytes']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backendTables = $backend->getTables();
        $this->assertCount(2, $backendTables);
    }

    /**
     * @dataProvider aliasSettingsProvider
     * @param array $aliasSettings
     */
    public function testCloneOtherAliasesNotAllowed(array $aliasSettings)
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $sourceBucketId,
            self::IMPORT_FILE_PATH
        );

        $aliasTableId = $this->_client->createAliasTable(
            $sourceBucketId,
            $sourceTableId,
            'aliased',
            $aliasSettings
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspacesClient->createWorkspace([
            'name' => 'cloning',
        ]);

        $this->expectException(Exception::class);
        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $aliasTableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);
    }

    public function testClonePreserveOffByDefault()
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $sourceBucketId,
            self::IMPORT_FILE_PATH
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspacesClient->createWorkspace([
            'name' => 'cloning',
        ]);

        // first load
        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);

        // second load will overwrite
        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languages-2',
                ],
            ],
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backendTables = $backend->getTables();
        $this->assertEquals(['languages-2'], $backendTables);
    }

    public function testClonePreserve()
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $sourceBucketId,
            self::IMPORT_FILE_PATH
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspacesClient->createWorkspace([
            'name' => 'cloning',
        ]);

        // first load
        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);

        // second load with preserve
        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languages-2',
                ],
            ],
            'preserve' => true,
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backendTables = $backend->getTables();
        $this->assertEquals(
            [
                'languages-2',
                'languagesDetails',
            ],
            $backendTables
        );
    }

    public function testTableAlreadyExistsAndOverwrite()
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace();

        $tableId = $this->createTableFromFile(
            $this->_client,
            $this->getTestBucketId(self::STAGE_IN),
            self::IMPORT_FILE_PATH
        );

        $tableSecondId = $this->createTableFromFile(
            $this->_client,
            $this->getTestBucketId(self::STAGE_IN),
            __DIR__ . '/../../_data/languages.more-rows.csv',
            'id',
            'languagesDetails2'
        );

        // first load
        $workspaces->cloneIntoWorkspace(
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

        $backend = new SnowflakeWorkspaceBackend($workspace);
        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(5, $workspaceTableData);

        // second load of same table with preserve
        try {
            $workspaces->cloneIntoWorkspace(
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

        try {
            // Invalid option combination
            $workspaces->cloneIntoWorkspace($workspace['id'], [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                        'overwrite' => true,
                    ],
                ],
                'preserve' => false,
            ]);
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestLogicalException', $e->getStringCode());
        }

        // third load table with more rows, preserve and overwrite
        $workspaces->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableSecondId,
                    'destination' => 'Langs',
                    'overwrite' => true,
                ],
            ],
            'preserve' => true,
        ]);

        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(6, $workspaceTableData);
    }

    public function aliasSettingsProvider()
    {
        return [
            'filtered alias' => [
                [
                    'aliasFilter' => [
                        'column' => 'id',
                        'values' => [26],
                    ],
                ],
            ],
            'selected columns' => [
                [
                    'aliasColumns' => [
                        'id',
                    ],
                ],
            ],
        ];
    }

    /**
     * @param Client $client
     * @param string $bucketId
     * @param string $importFilePath
     * @param string|array $primaryKey
     * @param string $tableName
     * @return string
     */
    private function createTableFromFile(
        Client $client,
        $bucketId,
        $importFilePath,
        $primaryKey = 'id',
        $tableName = 'languagesDetails'
    ) {

        return $client->createTable(
            $bucketId,
            $tableName,
            new CsvFile($importFilePath),
            ['primaryKey' => $primaryKey]
        );
    }

    private function createTableAliasChain($sourceTableId, $nestingLevel, $aliasNamePrefix)
    {
        $i = 0;
        while ($i < $nestingLevel) {
            $sourceTableId  = $this->_client->createAliasTable(
                $this->getTestBucketId(self::STAGE_OUT),
                $sourceTableId,
                sprintf('%s-%s', $aliasNamePrefix, $i)
            );
            $i++;
        }
        return $sourceTableId;
    }
}
