<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\MetadataUtils;

class BranchMetadataTest extends StorageApiTestCase
{
    use MetadataUtils;

    const TEST_METADATA = [
        [
            'key' => 'KBC.SomeEnity.metadataKey',
            'value' => 'some-value'
        ],
        [
            'key' => 'someMetadataKey',
            'value' => 'some-value'
        ]
    ];
    /** @var ClientProvider */
    private $clientProvider;

    /** @var BranchAwareClient */
    private $client;

    /**
     * @return void
     */
    public function setUp()
    {
        parent::setUp();

        $branches = new DevBranches($this->_client);
        $this->deleteBranchesByPrefix($branches, $this->generateBranchNameForParallelTest());

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->getDefaultBranchClient();

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
     * @return void
     */
    public function testCreateAndUpdateMetadata()
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
     * @return void
     */
    public function testManipulateMetadataRestrictionForReadOnlyUser()
    {
        // create read only client
        $readOnlyClient = $this->clientProvider->getDefaultBranchClient([
            'token' => STORAGE_API_READ_ONLY_TOKEN,
            'url' => STORAGE_API_URL,
        ]);
        // create metadata client
        $readOnlyMdClient = new DevBranchesMetadata($readOnlyClient);
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // list metadata
        $metadata = $readOnlyMdClient->listBranchMetadata();
        $this->assertCount(0, $metadata);

        // add metadata
        try {
            $readOnlyMdClient->addBranchMetadata(self::TEST_METADATA);
            $this->fail('should fail, insufficiently permission');
        } catch (ClientException $e) {
            $this->assertStringContainsString("You don't have access to resource", $e->getMessage());
            $this->assertSame(403, $e->getCode());
        }
    }

    /**
     * @return void
     */
    public function testSetMetadataEvent()
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);

        /** @var array $metadata */
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);

        $events = $this->listEvents($this->client, 'storage.devBranchMetadataSet');

        $this->assertEvent(
            $events[0],
            'storage.devBranchMetadataSet',
            sprintf('Development branch "%s" metadata set', 'Main'),
            $this->client->getCurrentBranchId(),
            'Main',
            'devBranch',
            [
                'metadata' => self::TEST_METADATA,
            ]
        );
    }

    /**
     * @return void
     */
    public function testDeleteMetadata()
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
     * @return void
     */
    public function testDeleteMetadataRestrictionForReadOnlyUser()
    {
        // create read only client
        $readOnlyClient = $this->clientProvider->getDefaultBranchClient([
            'token' => STORAGE_API_READ_ONLY_TOKEN,
            'url' => STORAGE_API_URL,
        ]);
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
            $this->assertStringContainsString("You don't have access to resource", $e->getMessage());
            $this->assertSame(403, $e->getCode());
        }
    }

    /**
     * @return void
     */
    public function testDeleteMetadataEvent()
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // add metadata
        /** @var array $metadata */
        $metadata = $defaultMdClient->addBranchMetadata(self::TEST_METADATA);
        $this->assertCount(2, $metadata);

        // delete metadata - first
        $defaultMdClient->deleteBranchMetadata($metadata[0]['id']);

        $events = $this->listEvents($this->client, 'storage.devBranchMetadataDeleted');

        $this->assertEvent(
            $events[0],
            'storage.devBranchMetadataDeleted',
            sprintf('Development branch "%s" metadata with key "%s" deleted', 'Main', $metadata[0]['key']),
            $this->client->getCurrentBranchId(),
            'Main',
            'devBranch',
            [
                'metadataId' => (int) $metadata[0]['id'],
                'key' => self::TEST_METADATA[0]['key'],
            ]
        );
    }
}
