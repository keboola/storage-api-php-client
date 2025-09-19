<?php

namespace Keboola\Test\Backend\Snowflake;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\SnowflakeWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;

class CloneIntoWorkspaceTest extends ParallelWorkspacesTestCase
{
    private const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    /**
     * @dataProvider cloneProvider
     */
    public function testClone(int $aliasNestingLevel, string $method, array $extraInput): void
    {
        $this->initEvents($this->workspaceSapiClient);

        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $sourceTableId = $this->createTableFromFile(
            $this->workspaceSapiClient,
            $bucketId,
            self::IMPORT_FILE_PATH,
        );

        $sourceTableId = $this->createTableAliasChain($sourceTableId, $aliasNestingLevel, 'languages');

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $runId = $this->workspaceSapiClient->generateRunId();
        $this->workspaceSapiClient->setRunId($runId);

        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                    ...$extraInput,
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
        $this->assertEventWithRetries($this->workspaceSapiClient, $assertCallback, $query);

        // test that stats are generated
        $stats = $this->workspaceSapiClient->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));
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
                $workspaceTableColumns,
            ),
        );

        $workspaceTableData = $backend->fetchAll('languagesDetails');
        $this->assertCount(5, $workspaceTableData);

        if ($aliasNestingLevel === 0) {
            //load table only for table and not for alias

            // try to import table from workspace
            // table languagesDetails has _timestamp column
            // but import still works
            $this->workspaceSapiClient->writeTableAsyncDirect(
                $sourceTableId,
                [
                    'dataWorkspaceId' => $workspace['id'],
                    'dataObject' => 'languagesDetails',
                    'columns' => ['id', 'name'],
                    'incremental' => true,
                ],
            );
        }

        // clone again but drop timestamp
        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetailsNoTimestamp',
                    'dropTimestampColumn' => true,
                    ...$extraInput,
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
                $workspaceTableColumns,
            ),
        );
    }

    /**
     * @return Generator<string, array{method: string, extraInputsOptions: array, operationName: string}>
     */
    public function cloneEndpointProvider(): Generator
    {
        yield 'cloneUsingCloneEndpoint' => [
            'method' => 'cloneIntoWorkspace',
            'extraInputsOptions' => [],
            'operationName' => 'workspaceLoadClone',
        ];
        yield 'cloneUsingLoadEndpoint' => [
            'method' => 'loadWorkspaceData',
            'extraInputsOptions' => [
                'loadType' => 'CLONE',
            ],
            'operationName' => 'workspaceLoad',
        ];
    }

    public function cloneProvider(): Generator
    {
        foreach ($this->cloneEndpointProvider() as $name => $data) {
            yield $name . ' - normal table' => [
                0,
                $data['method'],
                $data['extraInputsOptions'],
            ];
            yield $name . ' - simple alias' => [
                1,
                $data['method'],
                $data['extraInputsOptions'],
            ];
            yield $name . ' - simple alias 2 levels' => [
                2,
                $data['method'],
                $data['extraInputsOptions'],
            ];
        }
    }

    /**
     * @dataProvider cloneEndpointProvider
     */
    public function testCloneMultipleTables(string $method, array $extraInput, string $operationName): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $table1Id = $this->workspaceSapiClient->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH),
        );

        $table2Id = $this->workspaceSapiClient->createTableAsync(
            $bucketId,
            'rates',
            new CsvFile(__DIR__ . '/../../_data/rates.csv'),
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                    ...$extraInput,
                ],
                [
                    'source' => $table2Id,
                    'destination' => 'rates',
                    ...$extraInput,
                ],
            ],
        ]);

        $actualJob = null;
        foreach ($this->workspaceSapiClient->listJobs() as $job) {
            if ($job['operationName'] === $operationName) {
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
    public function testCloneOtherAliasesNotAllowed(array $aliasSettings, string $method, array $extraInput): void
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->workspaceSapiClient,
            $sourceBucketId,
            self::IMPORT_FILE_PATH,
        );

        $aliasTableId = $this->workspaceSapiClient->createAliasTable(
            $sourceBucketId,
            $sourceTableId,
            'aliased',
            $aliasSettings,
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $this->expectException(Exception::class);
        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $aliasTableId,
                    'destination' => 'languagesDetails',
                    ...$extraInput,
                ],
            ],
        ]);
    }

    /**
     * @dataProvider cloneEndpointProvider
     */
    public function testClonePreserveOffByDefault(string $method, array $extraInput): void
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->workspaceSapiClient,
            $sourceBucketId,
            self::IMPORT_FILE_PATH,
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        // first load
        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                    ...$extraInput,
                ],
            ],
        ]);

        // second load will overwrite
        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languages-2',
                    ...$extraInput,
                ],
            ],
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backendTables = $backend->getTables();
        $this->assertEquals(['languages-2'], $backendTables);
    }

    /**
     * @dataProvider cloneEndpointProvider
     */
    public function testClonePreserve(string $method, array $extraInput): void
    {
        $sourceBucketId = $this->getTestBucketId();
        $sourceTableId = $this->createTableFromFile(
            $this->workspaceSapiClient,
            $sourceBucketId,
            self::IMPORT_FILE_PATH,
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        // first load
        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languagesDetails',
                    ...$extraInput,
                ],
            ],
        ]);

        // second load with preserve
        $workspacesClient->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'languages-2',
                    ...$extraInput,
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
            $backendTables,
        );
    }

    /**
     * @dataProvider cloneEndpointProvider
     */
    public function testTableAlreadyExistsAndOverwrite(string $method, array $extraInput): void
    {
        $description = $this->generateDescriptionForTestObject();
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $client2 = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );

        $tableId = $this->createTableFromFile(
            $this->workspaceSapiClient,
            $this->getTestBucketId(self::STAGE_IN),
            self::IMPORT_FILE_PATH,
        );

        $testBucketName = $this->getTestBucketName($description, 'API-Shared-');
        $sharedBucket = $this->initEmptyBucket(
            $testBucketName,
            self::STAGE_IN,
            $description,
            $client2,
        );

        $this->dropBucketIfExists(
            $this->_client,
            self::STAGE_OUT . '.c-' . $testBucketName,
            true,
        );

        $this->createTableFromFile(
            $client2,
            $sharedBucket,
            __DIR__ . '/../../_data/languages.more-rows.csv',
            'id',
            'languagesDetails2',
        );

        $client2->shareOrganizationBucket($sharedBucket);

        $sourceProjectId = $client2->verifyToken()['owner']['id'];
        $linkedBucketId = $this->_client->linkBucket(
            $testBucketName,
            self::STAGE_OUT,
            $sourceProjectId,
            $sharedBucket,
        );

        // first load
        $workspaces->{$method}(
            $workspace['id'],
            [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                        ...$extraInput,
                    ],
                ],
            ],
        );

        $backend = new SnowflakeWorkspaceBackend($workspace);
        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(5, $workspaceTableData);

        // second load of same table with preserve
        try {
            $workspaces->{$method}(
                $workspace['id'],
                [
                    'input' => [
                        [
                            'source' => $tableId,
                            'destination' => 'Langs',
                            ...$extraInput,
                        ],
                    ],
                    'preserve' => true,
                ],
            );
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.duplicateTable', $e->getStringCode());
        }

        try {
            // Invalid option combination
            $workspaces->{$method}($workspace['id'], [
                'input' => [
                    [
                        'source' => $tableId,
                        'destination' => 'Langs',
                        'overwrite' => true,
                        ...$extraInput,
                    ],
                ],
                'preserve' => false,
            ]);
            $this->fail('table should not be created');
        } catch (ClientException $e) {
            $this->assertEquals('workspace.loadRequestLogicalException', $e->getStringCode());
        }

        // third load table with more rows, preserve and overwrite
        $workspaces->{$method}($workspace['id'], [
            'input' => [
                [
                    'source' => $linkedBucketId . '.languagesDetails2',
                    'destination' => 'Langs',
                    'overwrite' => true,
                    ...$extraInput,
                ],
            ],
            'preserve' => true,
        ]);

        $workspaceTableData = $backend->fetchAll('Langs');
        $this->assertCount(6, $workspaceTableData);
    }

    /**
     * @dataProvider cloneEndpointProvider
     */
    public function testCloneWithWrongInput(string $method): void
    {
        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        try {
            $workspacesClient->{$method}($workspace['id'], [
                'input' => 'this is not array',
            ]);
            $this->fail('Test should not reach this line');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
            $this->assertEquals(
                'This value should be of type array.',
                $e->getMessage(),
            );
        }
    }

    public function aliasSettingsProvider(): Generator
    {
        foreach ($this->cloneEndpointProvider() as $name => $data) {
            yield $name . ' - filtered alias' => [
                [
                    'aliasFilter' => [
                        'column' => 'id',
                        'values' => [26],
                    ],
                ],
                $data['method'],
                $data['extraInputsOptions'],
            ];
            yield $name . ' - selected columns' => [
                [
                    'aliasColumns' => [
                        'id',
                    ],
                ],
                $data['method'],
                $data['extraInputsOptions'],
            ];
        }
    }

    public function testQueueWorkspaceCloneInto(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $table1Id = $this->workspaceSapiClient->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH),
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $options = [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                ],
            ],
        ];

        $jobId = $workspacesClient->queueWorkspaceCloneInto($workspace['id'], $options);
        $job = $this->workspaceSapiClient->getJob($jobId);

        $this->assertEquals('workspaceLoadClone', $job['operationName']);
        $this->assertEquals($workspace['id'], $job['operationParams']['workspaceId']);
        $this->assertEquals($options['input'], $job['operationParams']['input']);
    }

    public function testCloneUsingLoadEndpoint(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $table1Id = $this->workspaceSapiClient->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH),
        );

        $workspacesClient = new Workspaces($this->workspaceSapiClient);
        $workspace = $this->initTestWorkspace();

        $options = [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                    'loadType' => 'CLONE',
                ],
            ],
        ];

        $jobId = $workspacesClient->queueWorkspaceLoadData($workspace['id'], $options);
        $job = $this->_client->getJob($jobId);
        $this->assertIsArray($job['operationParams']['input']);
        $this->assertIsArray($job['operationParams']['input'][0]);
        $this->assertArrayHasKey('loadType', $job['operationParams']['input'][0]);
        $this->assertSame('CLONE', $job['operationParams']['input'][0]['loadType']);

        // test load auto
        $options = [
            'input' => [
                [
                    'source' => $table1Id,
                    'destination' => 'languages',
                    'loadType' => 'AUTO',
                ],
            ],
        ];

        $jobId = $workspacesClient->queueWorkspaceLoadData($workspace['id'], $options);
        $job = $this->_client->getJob($jobId);
        $this->assertIsArray($job['operationParams']['input']);
        $this->assertIsArray($job['operationParams']['input'][0]);
        $this->assertArrayHasKey('loadType', $job['operationParams']['input'][0]);
        $this->assertSame('CLONE', $job['operationParams']['input'][0]['loadType']);
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
            ['primaryKey' => $primaryKey],
        );
    }

    private function createTableAliasChain($sourceTableId, $nestingLevel, $aliasNamePrefix)
    {
        $i = 0;
        while ($i < $nestingLevel) {
            $sourceTableId = $this->workspaceSapiClient->createAliasTable(
                $this->getTestBucketId(self::STAGE_OUT),
                $sourceTableId,
                sprintf('%s-%s', $aliasNamePrefix, $i),
            );
            $i++;
        }
        return $sourceTableId;
    }
}
