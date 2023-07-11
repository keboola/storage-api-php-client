<?php

namespace Keboola\Test\Backend\SOX;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\Test\StorageApiTestCase;
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
        // set tokenId to fetch events
        $this->tokenId = explode('-', STORAGE_API_DEVELOPER_TOKEN)[1];

        $runId = $privilegedClient->generateRunId();
        $branchClient->setRunId($runId);

        $event = $this->createAndWaitForEvent(
            (new Event())->setComponent('dummy')->setMessage('dummy'),
            $branchClient
        );
        $this->lastEventId = $event['id'];
        $newTableId = $branchClient->pullTableToBranch($productionTableId);
        // Check events created
        $bucketCreateEvent = $this->listEvents(
            $branchClient,
            'storage.bucketCreated',
            $productionBucketId,
            2
        );
        $this->assertCount(1, $bucketCreateEvent);
        $pullEvent = $this->listEvents($branchClient, 'storage.tableCopyToBucket');
        $this->assertCount(1, $pullEvent);
        $this->assertSame([
            'sourceBucketId' => $productionBucketId,
            'sourceBucketBranch' => $defaultBranch['id'],
            'destinationBucketId' => $productionBucketId,
            'destinationBucketBranch' => $newBranch['id'],
        ], $pullEvent[0]['params']);

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
        $events = $branchClient->listEvents([
            'sinceId' => $this->lastEventId,
            'limit' => 2,
            'q' => sprintf('token.id:%s', $this->tokenId),
        ]);
        $this->assertCount(1, $events);
        $this->assertSame([
            'sourceBucketId' => $productionBucketId,
            'sourceBucketBranch' => $defaultBranch['id'],
            'destinationBucketId' => $productionBucketId,
            'destinationBucketBranch' => $newBranch['id'],
        ], $events[0]['params']);

        $newTable = $branchClient->getTable($newTableId);
        $this->assertCount(1, $newTable['columnMetadata']);
        $this->assertArrayHasKey('id', $newTable['columnMetadata']);
        $this->assertCount(2, $newTable['columnMetadata']['id']);

        $this->assertCount(1, $newTable['metadata']);
        $this->assertSame('testvalTableUpdated', $newTable['metadata'][0]['value']);
    }
}
