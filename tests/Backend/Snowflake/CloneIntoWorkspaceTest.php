<?php



namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class CloneIntoWorkspaceTest extends WorkspacesTestCase
{
    const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    /**
     * @dataProvider cloneProvider
     * @param int $aliasNestingLevel
     * @throws Exception
     */
    public function testClone($aliasNestingLevel): void
    {
        $this->initEvents($this->_client);

        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );

        $sourceTableId = $this->createTableAliasChain($sourceTableId, $aliasNestingLevel, 'languages');

        $workspacesClient = new Workspaces($this->_client);

        $workspace = $workspacesClient->createWorkspace([], true);

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);

        // test that events are properly created
        $assertCallback = function ($events) use ($runId, $sourceTableId) {
            $this->assertCount(1, $events);
            $cloneEvent = array_pop($events);
            $this->assertSame('storage.workspaceTableCloned', $cloneEvent['event']);
            $this->assertSame($runId, $cloneEvent['runId']);
            $this->assertSame('storage', $cloneEvent['component']);
            $this->assertSame($sourceTableId, $cloneEvent['objectId']);
            $this->assertArrayHasKey('params', $cloneEvent);
            $this->assertSame($sourceTableId, $cloneEvent['params']['source']);
            $this->assertSame('languagesDetails', $cloneEvent['params']['destination']);
            $this->assertArrayHasKey('workspace', $cloneEvent['params']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.workspaceTableCloned')
            ->setTokenId($this->tokenId)
            ->setObjectId($sourceTableId)
            ->setRunId($runId);
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);

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
                ],
            ],
            array_map(
                static fn(array $column): array => [
                    'name' => $column['name'],
                    'type' => $column['type'],
                ],
                $workspaceTableColumns
            )
        );

        $workspaceTableData = $backend->fetchAll('languagesDetails');
        $this->assertCount(5, $workspaceTableData);

        // try to import table from workspace
        // table languagesDetails has _timestamp column
        // but import still works
        $this->_client->writeTableAsyncDirect(
            $sourceTableId,
            [
                'dataWorkspaceId' => $workspace['id'],
                'dataObject' => 'languagesDetails',
                'columns' => ['id', 'name'],
                'incremental' => true,
            ]
        );

        // clone again but drop timestamp
        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetailsNoTimestamp',
                    'dropTimestampColumn' => true,
                ],
            ],
        ]);
        $workspaceTableColumns = $backend->describeTableColumns('languagesDetailsNoTimestamp');
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
            ],
            array_map(
                static fn(array $column): array => [
                    'name' => $column['name'],
                    'type' => $column['type'],
                ],
                $workspaceTableColumns
            )
        );
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

    public function testCloneMultipleTables(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $table1Id = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH)
        );

        $table2Id = $this->_client->createTableAsync(
            $bucketId,
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv')
        );

        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace([], true);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
           'input' => [
               [
                   'source' => $table1Id,
                   'destination' => 'languages',
               ],
               [
                   'source' => $table2Id,
                   'destination' => 'rates',
               ],
           ],
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
    public function testCloneOtherAliasesNotAllowed(array $aliasSettings): void
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

        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace([], true);

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

    public function testClonePreserveOffByDefault(): void
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $sourceBucketId,
            self::IMPORT_FILE_PATH
        );

        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace([], true);

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

    public function testClonePreserve(): void
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $sourceBucketId,
            self::IMPORT_FILE_PATH
        );

        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace([], true);

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

    public function testTableAlreadyExistsAndOverwrite(): void
    {
        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace([], true);

        $client2 = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN
        );

        $tableId = $this->createTableFromFile(
            $this->_client,
            $this->getTestBucketId(self::STAGE_IN),
            self::IMPORT_FILE_PATH
        );

        $this->dropBucketIfExists($this->_client, 'out.c-linked-bucket', true);
        $this->dropBucketIfExists($client2, 'in.c-shared-bucket', true);

        $bucket = $client2->createBucket('shared-bucket', 'in');

        $this->createTableFromFile(
            $client2,
            $bucket,
            __DIR__ . '/../../_data/languages.more-rows.csv',
            'id',
            'languagesDetails2'
        );

        $client2->shareBucket($bucket);

        $sourceProjectId = $client2->verifyToken()['owner']['id'];
        $linkedBucketId = $this->_client->linkBucket(
            'linked-bucket',
            'out',
            $sourceProjectId,
            $bucket
        );

        // first load
        $workspaces->cloneIntoWorkspace(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                    ],
                ],
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
                        ],
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
                    'source' => $linkedBucketId . '.languagesDetails2',
                    'destination' => 'Langs',
                    'overwrite' => true,
                ],
            ],
            'preserve' => true,
        ]);

        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(6, $workspaceTableData);
    }

    public function testCloneWithWrongInput(): void
    {
        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace([], true);

        try {
            $workspacesClient->cloneIntoWorkspace($workspace['id'], [
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

    public function testQueueWorkspaceCloneInto(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $table1Id = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH)
        );

        $workspacesClient = new Workspaces($this->_client);
        $workspace = $workspacesClient->createWorkspace([], true);

        $options = [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                ],
            ],
        ];

        $jobId = $workspacesClient->queueWorkspaceCloneInto($workspace['id'], $options);
        $job = $this->_client->getJob($jobId);

        $this->assertEquals('workspaceLoadClone', $job['operationName']);
        $this->assertEquals($workspace['id'], $job['operationParams']['workspaceId']);
        $this->assertEquals($options['input'], $job['operationParams']['input']);
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

        return $client->createTableAsync(
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
