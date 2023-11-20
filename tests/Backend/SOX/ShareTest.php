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
        $name = $this->getTestBucketName($description);
        $bucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        $this->_client->shareOrganizationBucket($bucketId);

        self::assertTrue($this->_client->isSharedBucket($bucketId));
        $response = $this->_client->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();
        $clientInOtherProject = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN
        );
        $linkedBucketId = $clientInOtherProject->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        $clientInOtherProject->dropBucket($linkedBucketId, ['async' => true]);

        $this->_client->unshareBucket($bucketId, ['async' => true]);
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

        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);
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
            $client->linkBucket(
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
    public function testOtherCanShareAndLinkInBranch(Client $client)
    {
        $description = $this->generateDescriptionForTestObject();
        $branch = $this->branches->createBranch($description);
        $branchClient =$client->getBranchAwareClient($branch['id']);;

        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        try {
            $branchClient->shareOrganizationBucket($productionBucketId);
            $this->fail('Production manager should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }

        $this->_client->shareOrganizationBucket($productionBucketId);
        self::assertTrue($this->_client->isSharedBucket($productionBucketId));
        $response = $this->_client->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        try {
            $branchClient->linkBucket(
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
            $branchClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Production manager should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }
    }

    public function testPMCannotShareAndLinkInBranch()
    {
        $description = $this->generateDescriptionForTestObject();

        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        $branch = $this->branches->createBranch($description);
        $branchClient = $this->_client->getBranchAwareClient($branch['id']);;

        try {
            $branchClient->shareOrganizationBucket($productionBucketId);
            $this->fail('Production manager should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }

        $this->_client->shareOrganizationBucket($productionBucketId);
        self::assertTrue($this->_client->isSharedBucket($productionBucketId));
        $response = $this->_client->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        try {
            $branchClient->linkBucket(
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
            $branchClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Production manager should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }
    }

    public function initEmptyBucketInDefault(string $stage, string $name, string $description)
    {
        $client = $this->getDefaultBranchStorageApiClient();
        // unlink buckets
        foreach ($client->listBuckets() as $bucket) {
            if (!empty($bucket['sourceBucket'])) {
                $this->_client->dropBucket($bucket['id'], ['async' => true]);
            }
        }

        // unshare buckets
        foreach ($client->listBuckets() as $bucket) {
            if ($client->isSharedBucket($bucket['id'])) {
                $this->_client->unshareBucket($bucket['id']);
            }
        }

        return $this->initEmptyBucket($name, $stage, $description, $client);
    }
}
