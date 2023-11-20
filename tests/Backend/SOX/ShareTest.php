<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class ShareTest extends StorageApiTestCase
{
    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($developerClient);
        $this->cleanupTestBranches($developerClient);
    }

    public function testPMCanShareAndLinkInDefault()
    {
        $description = $this->generateDescriptionForTestObject();
        $privilegedClient = $this->getDefaultBranchStorageApiClient();

        $name = $this->getTestBucketName($description);
        $stage = self::STAGE_IN;
        $productionBucketId = $this->initEmptyBucketInDefault($privilegedClient, $stage, $name, $description);

        $this->_client->shareOrganizationBucket($productionBucketId);

        self::assertTrue($this->_client->isSharedBucket($productionBucketId));
        $response = $this->_client->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();
        $client2 = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN
        );
        $linkedBucketId = $client2->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        $client2->dropBucket($linkedBucketId, ['async' => true]);

        $this->_client->unshareBucket($productionBucketId, ['async' => true]);
    }

    public function tokensProvider(): \Generator
    {
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
        yield 'readOnly' => [
            $this->getReadOnlyStorageApiClient(),
        ];
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testOtherCannotShareAndLinkInMain(Client $client)
    {
        $description = $this->generateDescriptionForTestObject();
        $privilegedClient = $this->getDefaultBranchStorageApiClient();

        $name = $this->getTestBucketName($description);
        $stage = self::STAGE_IN;
        $productionBucketId = $this->initEmptyBucketInDefault($privilegedClient, $stage, $name, $description);
        try {
            $client->shareOrganizationBucket($productionBucketId);
            $this->fail('Others should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $this->_client->shareOrganizationBucket($productionBucketId);
        self::assertTrue($this->_client->isSharedBucket($productionBucketId));
        $response = $this->_client->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        try {
            $linkedBucketId = $client->linkBucket(
                $linkedBucketName,
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id']
            );
            $this->fail('Production manager should not be able to link bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        try {
            $client->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Others should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    /**
     * @dataProvider tokensProvider
     */
    // todo ma ist sharovanie a linkovanie v branchi
    public function testOtherCanShareAndLinkInBranch(Client $client)
    {
        $description = $this->generateDescriptionForTestObject();
        $branch = $this->branches->createBranch($description);
        $client2 =$client->getBranchAwareClient($branch['id']);;

        $privilegedClient = $this->getDefaultBranchStorageApiClient();

        $name = $this->getTestBucketName($description);
        $stage = self::STAGE_IN;
        $productionBucketId = $this->initEmptyBucketInDefault($privilegedClient, $stage, $name, $description);

        $client2->shareOrganizationBucket($productionBucketId);
        $this->_client->shareOrganizationBucket($productionBucketId);
        self::assertTrue($this->_client->isSharedBucket($productionBucketId));
        $response = $this->_client->listSharedBuckets();
//        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        $linkedBucketId = $client2->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );
    }

    public function testPMCannotShareAndLinkInBranch()
    {
        $description = $this->generateDescriptionForTestObject();
        $privilegedClient = $this->getDefaultBranchStorageApiClient();

        $name = $this->getTestBucketName($description);
        $stage = self::STAGE_IN;
        $productionBucketId = $this->initEmptyBucketInDefault($privilegedClient, $stage, $name, $description);

        $branch = $this->branches->createBranch($description);
        $client2 = $this->_client->getBranchAwareClient($branch['id']);;

        try {
            $client2->shareOrganizationBucket($productionBucketId);
            $this->fail('Production manager should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }

        $this->_client->shareOrganizationBucket($productionBucketId);
        self::assertTrue($this->_client->isSharedBucket($productionBucketId));
        $response = $this->_client->listSharedBuckets();
//        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        try {
            $linkedBucketId = $client2->linkBucket(
                $linkedBucketName,
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id']
            );
            $this->fail('Production manager should not be able to link bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        try {
            $client2->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Production manager should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }
    }

    public function initEmptyBucketInDefault(Client $client, string $stage, string $name, string $description)
    {
        // unlink buckets
        foreach ($this->_client->listBuckets() as $bucket) {
            if (!empty($bucket['sourceBucket'])) {
                $this->_client->dropBucket($bucket['id'], ['async' => true]);
            }
        }

        // unshare buckets
        foreach ($this->_client->listBuckets() as $bucket) {
            if ($this->_client->isSharedBucket($bucket['id'])) {
                $this->_client->unshareBucket($bucket['id']);
            }
        }

        $productionBucket = $client->getBucket("$stage.c-$name");
         if ($productionBucket) {
            $client->dropBucket($productionBucket['id'], ['async' => true]);
        }
        return $client->createBucket($name, $stage, $description);
    }
}
