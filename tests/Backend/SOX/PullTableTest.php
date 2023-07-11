<?php

namespace Keboola\Test\Backend\SOX;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\MetadataUtils;

class PullTableTest extends StorageApiTestCase
{
    use MetadataUtils;

    private Client $developerClient;

    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $this->developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($this->developerClient);
        foreach ($this->getBranchesForCurrentTestCase() as $branch) {
            $this->branches->deleteBranch($branch['id']);
        }
    }

    private function getBranchesForCurrentTestCase(): array
    {
        $prefix = $this->generateDescriptionForTestObject();
        $branches = [];
        foreach ($this->branches->listBranches() as $branch) {
            if (str_starts_with($branch['name'], $prefix)) {
                $branches[] = $branch;
            }
        }
        return $branches;
    }

    public function testPullTableFromDefaultBranch(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject());

        $privilegedClient = $this->getDefaultBranchStorageApiClient();
        $productionBucketId = $this->initEmptyBucket(
            $this->getTestBucketName($description),
            self::STAGE_IN,
            $description,
            $privilegedClient
        );
        $productionTableId = $privilegedClient->createTableAsync(
            $productionBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $productionTable = $privilegedClient->getTable($productionTableId);
        $metadata = new Metadata($privilegedClient);
        $metadata->postTableMetadataWithColumns(
            new TableMetadataUpdateOptions(
                $productionTableId,
                'test',
                [
                    [
                        'key' => 'key1',
                        'value' => 'testvalTable',
                    ],
                ],
                [
                    'id' => [
                        [
                            'key' => 'key1',
                            'value' => 'testvalCol',
                        ],
                    ],
                ]
            )
        );

        $branchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);
        $tokenId = explode('-', STORAGE_API_DEVELOPER_TOKEN)[1];
        $event = $this->createAndWaitForEvent(
            (new Event())->setComponent('dummy')->setMessage('dummy'),
            $branchClient
        );
        $this->lastEventId = $event['id'];
        $newTableId = $branchClient->pullTableToBranch($productionTableId);

        // Check events created
        $assertCallback = function ($events) use ($productionBucketId) {
            $this->assertCount(1, $events);
            // check bucket event
            $this->assertSame('storage.bucketCreated', $events[0]['event']);
            $this->assertSame($productionBucketId, $events[0]['objectId']);
            $this->assertSame('bucket', $events[0]['objectType']);
        };
        $query = new EventsQueryBuilder();
        $query->setTokenId($tokenId);
        $query->setEvent('storage.bucketCreated');
        $this->assertEventWithRetries($branchClient, $assertCallback, $query);

        // Check events created
        $assertCallback = function ($events) use ($productionBucketId, $defaultBranch, $newBranch, $productionTable) {
            $this->assertCount(1, $events);
            // check table event
            $this->assertSame('storage.tableCopyToBucket', $events[0]['event']);
            $this->assertSame($productionTable['id'], $events[0]['objectId']);
            $this->assertSame([
                'sourceBucketId' => $productionBucketId,
                'sourceBucketBranch' => $defaultBranch['id'],
                'destinationBucketId' => $productionBucketId,
                'destinationBucketBranch' => $newBranch['id'],
            ], $events[0]['params']);
        };
        $query = new EventsQueryBuilder();
        $query->setTokenId($tokenId);
        $query->setEvent('storage.tableCopyToBucket');
        $this->assertEventWithRetries($branchClient, $assertCallback, $query);

        // check table created
        $newTable = $branchClient->getTable($newTableId);
        $this->assertNotSame($productionTable['created'], $newTable['created']);
        $this->assertSame($newBranch['id'], $newTable['bucket']['idBranch']);
        $this->assertCount(1, $newTable['columnMetadata']);
        $this->assertArrayHasKey('id', $newTable['columnMetadata']);
        $this->assertCount(1, $newTable['columnMetadata']['id']);
        $this->assertSame('testvalCol', $newTable['columnMetadata']['id'][0]['value']);
        $this->assertCount(1, $newTable['metadata']);
        $this->assertSame('testvalTable', $newTable['metadata'][0]['value']);

        // post new metadata and pull again
        $metadata->postTableMetadataWithColumns(
            new TableMetadataUpdateOptions(
                $productionTableId,
                'test',
                [
                    [
                        'key' => 'key1',
                        'value' => 'testvalTableUpdated',
                    ],
                ],
                [
                    'id' => [
                        [
                            'key' => 'key1',
                            'value' => 'testvalCol',
                        ],
                        [
                            'key' => 'key2',
                            'value' => 'testvalCol2',
                        ],
                    ],
                ]
            )
        );

        $event = $this->createAndWaitForEvent(
            (new Event())->setComponent('dummy')->setMessage('dummy'),
            $branchClient
        );
        $this->lastEventId = $event['id'];
        $newTableId = $branchClient->pullTableToBranch($productionTableId);
        // Check events created
        // note that there is no bucket create event since bucket already exists
        $assertCallback = function ($events) use ($productionBucketId, $defaultBranch, $newBranch, $productionTable) {
            $this->assertCount(1, $events);
            // check table event
            $this->assertSame('storage.tableCopyToBucket', $events[0]['event']);
            $this->assertSame($productionTable['id'], $events[0]['objectId']);
            $this->assertSame([
                'sourceBucketId' => $productionBucketId,
                'sourceBucketBranch' => $defaultBranch['id'],
                'destinationBucketId' => $productionBucketId,
                'destinationBucketBranch' => $newBranch['id'],
            ], $events[0]['params']);
        };
        $query = new EventsQueryBuilder();
        $query->setTokenId($tokenId);
        $query->setEvent('storage.tableCopyToBucket');
        $this->assertEventWithRetries($branchClient, $assertCallback, $query);

        $newTable = $branchClient->getTable($newTableId);
        $this->assertCount(1, $newTable['columnMetadata']);
        $this->assertArrayHasKey('id', $newTable['columnMetadata']);
        $this->assertCount(2, $newTable['columnMetadata']['id']);

        $this->assertCount(1, $newTable['metadata']);
        $this->assertSame('testvalTableUpdated', $newTable['metadata'][0]['value']);

        // try add new column
        $privilegedClient->addTableColumn($productionTableId, 'newCol');
        $table = $branchClient->getTable($newTableId);
        $this->assertCount(2, $table['columns']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertCount(3, $table['columns']);
        $newTableId = $branchClient->pullTableToBranch($productionTableId);
        $table = $branchClient->getTable($newTableId);
        $this->assertCount(3, $table['columns']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertCount(3, $table['columns']);

        //try drop column
        $privilegedClient->deleteTableColumn($productionTableId, 'newCol');
        $table = $branchClient->getTable($newTableId);
        $this->assertCount(3, $table['columns']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertCount(2, $table['columns']);
        $newTableId = $branchClient->pullTableToBranch($productionTableId);
        $table = $branchClient->getTable($newTableId);
        $this->assertCount(2, $table['columns']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertCount(2, $table['columns']);
    }

    public function testPullTypedTableFromDefaultBranch(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject());

        $privilegedClient = $this->getDefaultBranchStorageApiClient();
        $productionBucketId = $this->initEmptyBucket(
            $this->getTestBucketName($description),
            self::STAGE_IN,
            $description,
            $privilegedClient
        );
        $productionTableId = $privilegedClient->createTableDefinition(
            $productionBucketId,
            [
                'name' => 'languages',
                'primaryKeysNames' => [],
                'columns' => [
                    [
                        'name' => 'id',
                        'definition' => [
                            'type' => 'INT',
                            'nullable' => false,
                        ],
                    ],
                    [
                        'name' => 'name',
                        'definition' => [
                            'type' => 'VARCHAR',
                        ],
                    ],
                ],
            ]
        );
        $branchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);
        $newTableId = $branchClient->pullTableToBranch($productionTableId);

        // clone table to WS so we can check the column type
        $wsApi = new Workspaces($branchClient);
        $ws = $wsApi->createWorkspace();
        $wsApi->cloneIntoWorkspace($ws['id'], [
            'input' => [
                [
                    'source' => $newTableId,
                    'destination' => 'test',
                    'dropTimestampColumn' => true,
                ],
            ],
        ]);
        $backend = WorkspaceBackendFactory::createWorkspaceForSnowflakeDbal($ws);
        $ref = $backend->getTableReflection('test');
        /** @var SnowflakeColumn[] $defs */
        $defs = iterator_to_array($ref->getColumnsDefinitions());
        $this->assertSame('NUMBER', $defs[0]->getColumnDefinition()->getType());
        $this->assertSame('38,0', $defs[0]->getColumnDefinition()->getLength());

        // check table created
        $newTable = $branchClient->getTable($newTableId);
        $expectedColumnsDefinitions = [
            [
                'name' => 'id',
                'definition' => [
                    'type' => 'NUMBER',
                    'nullable' => false,
                    'length' => '38,0',
                ],
                'basetype' => 'NUMERIC',
                'canBeFiltered' => true,
            ],
            [
                'name' => 'name',
                'definition' => [
                    'type' => 'VARCHAR',
                    'nullable' => true,
                    'length' => '16777216',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
        ];
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitions,
        ], $newTable['definition']);

        // try add new column
        $expectedColumnsDefinitionsExtraColumn = [
            ...$expectedColumnsDefinitions,
            [
                'name' => 'newCol',
                'definition' => [
                    'type' => 'VARCHAR',
                    'nullable' => true,
                    'length' => '16777216',
                ],
                'basetype' => 'STRING',
                'canBeFiltered' => true,
            ],
        ];

        $privilegedClient->addTableColumn($productionTableId, 'newCol', null, 'STRING');
        $table = $branchClient->getTable($newTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitions,
        ], $table['definition']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitionsExtraColumn,
        ], $table['definition']);
        $newTableId = $branchClient->pullTableToBranch($productionTableId);
        $table = $branchClient->getTable($newTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitionsExtraColumn,
        ], $table['definition']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitionsExtraColumn,
        ], $table['definition']);

        // try drop column
        $privilegedClient->deleteTableColumn($productionTableId, 'newCol');
        $table = $branchClient->getTable($newTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitionsExtraColumn,
        ], $table['definition']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitions,
        ], $table['definition']);
        $newTableId = $branchClient->pullTableToBranch($productionTableId);
        $table = $branchClient->getTable($newTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitions,
        ], $table['definition']);
        $table = $privilegedClient->getTable($productionTableId);
        $this->assertSame([
            'primaryKeysNames' => [],
            'columns' => $expectedColumnsDefinitions,
        ], $table['definition']);
    }
}
