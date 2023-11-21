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
    }

    public function testPMCanShareAndLinkInDefault(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $name = $this->getTestBucketName($description);
        $bucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        // test PM can share bucket
        $this->_client->shareOrganizationBucket($bucketId);

        self::assertTrue($this->_client->isSharedBucket($bucketId));
        $response = $this->_client->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();
        $clientInOtherProject = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN
        );
        // test PM can link bucket
        $linkedBucketId = $clientInOtherProject->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        $linkedBucket = $clientInOtherProject->getBucket($linkedBucketId);
        $this->assertSame($linkedBucketId, $linkedBucket['id']);

        // test PM can unlink bucket
        $clientInOtherProject->dropBucket($linkedBucketId, ['async' => true]);

        try {
            $clientInOtherProject->getBucket($linkedBucketId);
            $this->fail('Bucket should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame(sprintf('Bucket %s not found', $linkedBucketId), $e->getMessage());
        }

        // test PM can unshare bucket
        $this->_client->unshareBucket($bucketId, ['async' => true]);
        $response = $this->_client->listSharedBuckets();
        self::assertCount(0, $response);
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
    public function testOtherCannotShareAndLinkInMain(Client $client): void
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

        $linkedBucketId = $this->_client->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        try {
            $client->dropBucket($linkedBucketId, ['async' => true]);
            $this->fail('Others should not be able to unlink bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $this->_client->dropBucket($linkedBucketId, ['async' => true]);

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
    public function testOtherCannotShareAndLinkInBranch(Client $client): void
    {
        $description = $this->generateDescriptionForTestObject();
        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        $branch = $this->branches->createBranch($description);
        $branchClient = $client->getBranchAwareClient($branch['id']);
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

        // try share branch bucket
        $devBucketName = 'dev-bucket-' . sha1($this->generateDescriptionForTestObject());
        // always use developer client to create bucket in branch to work around read-only role
        $devBucketId = $this->getDeveloperStorageApiClient()->getBranchAwareClient($branch['id'])->createBucket(
            $devBucketName,
            'in',
        );
        try {
            $branchClient->shareOrganizationBucket($devBucketId);
            $this->fail('Others should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }

        // validate cannot link bucket from main in branch
        try {
            $branchClient->linkBucket(
                $linkedBucketName,
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id']
            );
            $this->fail('Others should not be able to link bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        // validate cannot un-share bucket from main in branch
        try {
            $branchClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Production manager should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }
    }

    public function testPMCannotShareAndLinkInBranch(): void
    {
        $description = $this->generateDescriptionForTestObject();

        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        // todo zvalidovat, ze sa po vytvoreni branche vytvori buctket, alebo je nejak dostupny v branchi
        $branch = $this->branches->createBranch($description);
        $branchClient = $this->_client->getBranchAwareClient($branch['id']);

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

        // todo try link branch bucket
        $devBucketName = 'dev-bucket-' . sha1($this->generateDescriptionForTestObject());
        $pmBranchAwareClient = $this->_client->getBranchAwareClient($branch['id']);
        // always use developer client to create bucket in branch to work around PM role limitations in dev branch
        $devBucketId = $this->getDeveloperStorageApiClient()->getBranchAwareClient($branch['id'])->createBucket(
            $devBucketName,
            'in',
        );
        try {
            $pmBranchAwareClient->shareOrganizationBucket($devBucketId);
            $this->fail('Production manager should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }
        // validate cannot link bucket from main in branch
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

        // validate cannot un-share bucket from main in branch
        try {
            $branchClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Production manager should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(501, $e->getCode());
            $this->assertSame('Not implemented', $e->getMessage());
        }
    }

    public function initEmptyBucketInDefault(string $stage, string $name, string $description): string
    {
        $client = $this->getDefaultBranchStorageApiClient();
        return $this->initEmptyBucket($name, $stage, $description, $client);
    }
}
