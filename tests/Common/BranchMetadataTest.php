<?php

namespace Keboola\Test\Common;

use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\DevBranchesMetadata;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;

class BranchMetadataTest extends StorageApiTestCase
{
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

        // TODO cleanup default branch

        $branches = new DevBranches($this->_client);
        $this->deleteBranchesByPrefix($branches, $this->generateBranchNameForParallelTest());

        $this->clientProvider = new ClientProvider($this);
        $this->client = $this->clientProvider->getDefaultBranchClient();

        $this->initEvents($this->client);
    }

    /**
     * @return void
     */
    public function testCreateAndUpdateMetadata()
    {
        // create metadata client
        $defaultMdClient = new DevBranchesMetadata($this->client);

        // list metadata
        $metadata = $defaultMdClient->listBranchMetadata();
        // TODO change after DELETE is ready
        //$this->assertCount(2, $metadata);
        $this->assertIsArray($metadata);

        // add metadata
        /** @var array $metadata */
        $metadata = $defaultMdClient->postBranchMetadata(self::TEST_METADATA);
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
        /** @var array $newMetadata */
        $newMetadata = $defaultMdClient->postBranchMetadata($updatedMetadata);
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
        $defaultMdClient = new DevBranchesMetadata($readOnlyClient);

        // list metadata
        $metadata = $defaultMdClient->listBranchMetadata();
        // TODO change after DELETE is ready
        //$this->assertCount(2, $metadata);
        $this->assertIsArray($metadata);

        // add metadata
        try {
            $metadata = $defaultMdClient->postBranchMetadata(self::TEST_METADATA);
            $this->fail('should fail, insufficiently permission');
        } catch (ClientException $e) {
            $this->assertContains("You don't have access to resource", $e->getMessage());
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

        $metadata = $defaultMdClient->postBranchMetadata(self::TEST_METADATA);
        // TODO change after DELETE is ready
        //$this->assertCount(2, $metadata);
        $this->assertIsArray($metadata);

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
    private function assertMetadataEquals(array $expected, array $actual)
    {
        foreach ($expected as $key => $value) {
            self::assertArrayHasKey($key, $actual);
            self::assertSame($value, $actual[$key]);
        }
        self::assertArrayHasKey('timestamp', $actual);
    }
}
