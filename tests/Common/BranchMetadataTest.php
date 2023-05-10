<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;
use Keboola\Test\Utils\MetadataUtils;

class BranchMetadataTest extends StorageApiTestCase
{
    use MetadataUtils;
    use EventTesterUtils;

    const TEST_METADATA = [
        [
            'key' => 'KBC.SomeEnity.metadataKey',
            'value' => 'some-value',
        ],
        [
            'key' => 'someMetadataKey',
            'value' => 'some-value',
        ],
    ];
    /** @var ClientProvider */
    private $clientProvider;

    /** @var BranchAwareClient */
    private $client;

    /**
     * @return void
     */
    public function setUp(): void
    {
        parent::setUp();

        $branches = new DevBranches($this->_client);
        $this->deleteBranchesByPrefix($branches, $this->generateBranchNameForParallelTest());

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->createBranchAwareClientForCurrentTest();

        $this->cleanupBranchMetadata($this->client);

        $this->initEvents($this->client);
    }

    /**
     * @return void
     */
    private function cleanupBranchMetadata(BranchAwareClient $client)
    {
        $mdClient = new DevBranchesMetadata($client);

        /** @var array $all */
        $all = $mdClient->listBranchMetadata();

        foreach ($all as $md) {
            $mdClient->deleteBranchMetadata($md['id']);
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testCreateAndUpdateMetadata(): void
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // list metadata
        /** @var array $metadata */
        $metadata = $defaultMdClient->listBranchMetadata();
        $this->assertCount(0, $metadata);

        // add metadata
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $metadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $metadata[1]);

        // wait for other timestamp
        sleep(1);

        // update metadata
        $updatedMetadata = [
            [
                'key' => 'KBC.SomeEnity.metadataKey',
                'value' => 'some-value-2',
            ],
        ];
        $newMetadata = $defaultMdClient->addBranchMetadata($updatedMetadata);
        $this->assertCount(2, $newMetadata);
        $this->assertMetadataEquals($updatedMetadata[0], $newMetadata[0]);
        $this->assertNotSame($metadata[0]['timestamp'], $newMetadata[0]['timestamp']);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testManipulateMetadataRestrictionForReadOnlyUser(): void
    {
        // create read only client
        $readOnlyClient = $this->clientProvider->createBranchAwareClientForCurrentTest([
            'token' => STORAGE_API_READ_ONLY_TOKEN,
            'url' => STORAGE_API_URL,
        ], true);
        // create metadata client
        $readOnlyMdClient = new DevBranchesMetadata($readOnlyClient);

        // list metadata
        $metadata = $readOnlyMdClient->listBranchMetadata();
        $this->assertCount(0, $metadata);

        // add metadata
        try {
            $readOnlyMdClient->addBranchMetadata(self::TEST_METADATA);
            $this->fail('should fail, insufficiently permission');
        } catch (ClientException $e) {
            $this->assertStringContainsString("You don't have access to the resource", $e->getMessage());
            $this->assertSame(403, $e->getCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testSetMetadataEvent(): void
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);
        $currentBranchName = $this->getCurrentDevBranchName((int) $this->client->getCurrentBranchId());

        /** @var array $metadata */
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);

        $assertCallback = function ($events) use ($currentBranchName) {
            $this->assertCount(1, $events);
            $this->assertEvent(
                $events[0],
                'storage.devBranchMetadataSet',
                sprintf('Development branch "%s" metadata set', $currentBranchName),
                $this->client->getCurrentBranchId(),
                $currentBranchName,
                'devBranch',
                [
                    'metadata' => self::TEST_METADATA,
                ]
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.devBranchMetadataSet')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testDeleteMetadata(): void
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // list metadata
        /** @var array $metadata */
        $metadata = $defaultMdClient->listBranchMetadata();
        $this->assertCount(0, $metadata);

        // add metadata
        /** @var array $metadata */
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);

        // delete metadata - first
        $defaultMdClient->deleteBranchMetadata($metadata[0]['id']);

        // list metadata
        /** @var array $deletedMetadata */
        $deletedMetadata = $defaultMdClient->listBranchMetadata();
        $this->assertCount(1, $deletedMetadata);
        // check there is not deleted one
        $this->assertNotSame($metadata[0]['id'], $deletedMetadata[0]['id']);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $deletedMetadata[0]);

        // delete metadata - first again
        try {
            $defaultMdClient->deleteBranchMetadata($metadata[0]['id']);
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertStringContainsString(sprintf('Metadata with id "%s" not found', $metadata[0]['id']), $e->getMessage());
            $this->assertSame(404, $e->getCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testDeleteMetadataRestrictionForReadOnlyUser(): void
    {
        // create read only client
        $readOnlyClient = $this->clientProvider->createBranchAwareClientForCurrentTest([
            'token' => STORAGE_API_READ_ONLY_TOKEN,
            'url' => STORAGE_API_URL,
        ], true);
        // create metadata client
        $readOnlyMdClient = new DevBranchesMetadata($readOnlyClient);
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // list metadata
        /** @var array $metadata */
        $metadata = $readOnlyMdClient->listBranchMetadata();
        $this->assertCount(0, $metadata);

        // add metadata to delete
        /** @var array $metadata */
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);

        // delete metadata
        try {
            $readOnlyMdClient->deleteBranchMetadata($metadata[0]['id']);
            $this->fail('should fail, insufficiently permission');
        } catch (ClientException $e) {
            $this->assertStringContainsString("You don't have access to the resource", $e->getMessage());
            $this->assertSame(403, $e->getCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientType
     * @return void
     */
    public function testDeleteMetadataEvent(): void
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);
        $currentBranchName = $this->getCurrentDevBranchName((int) $this->client->getCurrentBranchId());

        // add metadata
        /** @var array $metadata */
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);

        // delete metadata - first
        $defaultMdClient->deleteBranchMetadata((int) $metadata[0]['id']);

        $assertCallback = function ($events) use ($currentBranchName, $metadata) {
            $this->assertCount(1, $events);
            $this->assertEvent(
                $events[0],
                'storage.devBranchMetadataDeleted',
                sprintf('Development branch "%s" metadata with key "%s" deleted', $currentBranchName, $metadata[0]['key']),
                $this->client->getCurrentBranchId(),
                $currentBranchName,
                'devBranch',
                [
                    'metadataId' => (int) $metadata[0]['id'],
                    'key' => self::TEST_METADATA[0]['key'],
                ]
            );
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.devBranchMetadataDeleted')
            ->setTokenId($this->tokenId);
        $this->assertEventWithRetries($this->client, $assertCallback, $query);
    }

    /**
     * @return void
     */
    public function testCreateBranchMetadataCopyToBranch(): void
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // list metadata in default
        $metadata = $defaultMdClient->listBranchMetadata();
        $this->assertCount(0, $metadata);

        // add metadata to default
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $metadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $metadata[1]);

        // wait for other timestamp
        sleep(1);

        // create new branch
        $branchClient = $this->clientProvider->getDevBranchClient();
        $branchMdClient = new DevBranchesMetadata($branchClient);

        // list copied metadata in branch
        $branchMetadata = $branchMdClient->listBranchMetadata();
        $this->assertCount(2, $branchMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $branchMetadata[0]);
        $this->assertGreaterThan($metadata[0]['timestamp'], $branchMetadata[0]['timestamp']);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $branchMetadata[1]);
        $this->assertNotSame($metadata[0]['timestamp'], $branchMetadata[0]['timestamp']);
        $lastBranchMetadata = $branchMetadata;

        // add new metadata to default
        $DEFAULT_METADATA = [
            [
                'key' => 'default-key',
                'value' => 'Default value',
            ],
        ];
        $metadata = $defaultMdClient->addBranchMetadata($DEFAULT_METADATA);
        $this->assertCount(3, $metadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $metadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $metadata[1]);
        $this->assertMetadataEquals($DEFAULT_METADATA[0], $metadata[2]);

        // delete some metadata from default
        $defaultMdClient->deleteBranchMetadata((int) $metadata[0]['id']);

        // list metadata in default
        $lastDefaultMetadata = $defaultMdClient->listBranchMetadata();
        $this->assertCount(2, $lastDefaultMetadata);

        // check metadata are untouched in branch
        $branchMetadata = $branchMdClient->listBranchMetadata();
        $this->assertSame($lastBranchMetadata, $branchMetadata);

        // add new metadata to branch
        $BRANCH_METADATA = [
            [
                'key' => 'branch-key',
                'value' => 'Branch value',
            ],
        ];
        $branchMetadata = $branchMdClient->addBranchMetadata($BRANCH_METADATA);
        $this->assertCount(3, $branchMetadata);
        $this->assertMetadataEquals(self::TEST_METADATA[0], $branchMetadata[0]);
        $this->assertMetadataEquals(self::TEST_METADATA[1], $branchMetadata[1]);
        $this->assertMetadataEquals($BRANCH_METADATA[0], $branchMetadata[2]);

        // delete some metadata from branch
        $branchMdClient->deleteBranchMetadata((int) $branchMetadata[0]['id']);

        // check metadata are untouched in default
        $defaultMetadata = $defaultMdClient->listBranchMetadata();
        $this->assertSame($lastDefaultMetadata, $defaultMetadata);

        // delete branch
        $defaultClient = new DevBranches($this->_client);
        $defaultClient->deleteBranch((int) $branchClient->getCurrentBranchId());

        // check metadata not exists in branch
        try {
            $branchMdClient->listBranchMetadata();
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }

        // check metadata are untouched in default
        $defaultMetadata = $defaultMdClient->listBranchMetadata();
        $this->assertSame($lastDefaultMetadata, $defaultMetadata);
    }
}
