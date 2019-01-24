<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Exception;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

class CloneIntoWorkspaceTest extends WorkspacesTestCase
{
    const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    /**
     * @var Client
     */
    private $_client2;

    public function setUp()
    {
        $this->_client2 = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ));

        foreach ($this->_client2->listBuckets() as $bucket) {
            $this->_client2->dropBucket($bucket['id']);
        }

        parent::setUp();
    }

    /**
     * @dataProvider cloneProvider
     * @param bool $isSourceTableAlias
     * @throws Exception
     */
    public function testClone(bool $isSourceTableAlias): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );

        if ($isSourceTableAlias) {
            $bucketId = $this->getTestBucketId(self::STAGE_OUT);
            $sourceTableId = $this->_client->createAliasTable($bucketId, $sourceTableId);
        }


        $workspacesClient = new Workspaces($this->_client);

        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

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
        $this->assertSame('in.c-API-tests.languagesDetails', $cloneEvent['params']['source']);
        $this->assertSame('languagesDetails', $cloneEvent['params']['destination']);
        $this->assertArrayHasKey('sourceProject', $cloneEvent['params']);
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
              false,
          ],
          'simple alias' => [
              true,
          ]
        ];
    }

    public function testCloneMultipleTables(): void
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

        $workspacesClient = new Workspaces($this->_client);
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

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $backendTables = $backend->getTables();
        $this->assertCount(2, $backendTables);
    }

    public function testCloneLinkedTables()
    {
        $sourceProject = $this->_client->verifyToken()['owner'];

        //setup test tables
        $sourceBucketId1 = $this->getTestBucketId(self::STAGE_IN);
        $sourceBucketId2 = $this->getTestBucketId(self::STAGE_OUT);

        $this->_client->createTable(
            $sourceBucketId1,
            'languages',
            new CsvFile(self::IMPORT_FILE_PATH)
        );

        $this->_client->createTable(
            $sourceBucketId2,
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        // share and link bucket
        $this->_client->shareBucket($sourceBucketId1, ['sharing' => 'organization-project']);
        $bucketId1 = $this->_client2->linkBucket('linked', self::STAGE_IN, $sourceProject['id'], $sourceBucketId1);

        // share, link and inlink second bucket - test that it doesn't break permissions of first linked bucket
        $this->_client->shareBucket($sourceBucketId2, ['sharing' => 'organization-project']);
        $bucketId2 = $this->_client2->linkBucket('linked', self::STAGE_OUT, $sourceProject['id'], $sourceBucketId2);
        $this->_client2->dropBucket($bucketId2);

        // init workspace
        $workspacesClient = new Workspaces($this->_client2);
        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $bucketId1 . '.languages',
                    'destination' => 'languages-renamed',
                ],
            ]
        ]);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));
        $events = $this->_client2->listEvents([
            'runId' => $runId,
        ]);

        // there are two events, dummy (0) and the clone event (1)
        $cloneEvent = array_pop($events);

        $this->assertArrayHasKey('params', $cloneEvent);
        $cloneEventParams = $cloneEvent['params'];

        $this->assertSame('storage.workspaceTableCloned', $cloneEvent['event']);
        $this->assertSame($runId, $cloneEvent['runId']);
        $this->assertSame('storage', $cloneEvent['component']);
        $this->assertSame($bucketId1 . '.languages', $cloneEvent['objectId']);
        $this->assertSame('in.c-API-tests.languages', $cloneEventParams['source']);
        $this->assertSame('languages-renamed', $cloneEventParams['destination']);

        $this->assertArrayHasKey('source', $cloneEventParams);
        $this->assertSame($bucketId1 . '.languages', $cloneEvent['objectId']);
        $this->assertArrayHasKey('sourceProject', $cloneEventParams);
        $this->assertSame($sourceProject['id'], $cloneEventParams['sourceProject']['id']);
        $this->assertSame($sourceProject['name'], $cloneEventParams['sourceProject']['name']);

        $this->assertArrayHasKey('workspace', $cloneEventParams);
        $this->assertSame($workspace['id'], $cloneEventParams['workspace']['id']);
        $this->assertSame($workspace['name'], $cloneEventParams['workspace']['name']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        // check that the table is in the workspace
        $backendTables = $backend->getTables();
        $this->assertCount(1, $backendTables);
        $this->assertContains($backend->toIdentifier("languages-renamed"), $backendTables);

        // check table structure and data
        $workspaceTableColumns = $backend->describeTableColumns('languages-renamed');
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

        $workspaceTableData = $backend->fetchAll('languages-renamed"');
        $this->assertCount(5, $workspaceTableData);
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

        $workspacesClient = new Workspaces($this->_client);
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

        $workspacesClient = new Workspaces($this->_client);
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

    public function aliasSettingsProvider(): array
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
     * @param array|string $primaryKey
     */
    private function createTableFromFile(
        Client $client,
        string $bucketId,
        string $importFilePath,
        $primaryKey = 'id'
    ): string {

        return $client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFilePath),
            ['primaryKey' => $primaryKey]
        );
    }
}
