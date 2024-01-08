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

        $clientInOtherProject = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );
        foreach ([$clientInOtherProject, $this->_client] as $client) {
            // unlink buckets
            foreach ($client->listBuckets() as $bucket) {
                if (!empty($bucket['sourceBucket'])) {
                    $client->dropBucket($bucket['id']);
                }
            }
        }
        foreach ([$clientInOtherProject, $this->_client] as $client) {
            // unshare buckets
            foreach ($client->listBuckets() as $bucket) {
                if ($client->isSharedBucket($bucket['id'])) {
                    $client->unshareBucket($bucket['id']);
                }
            }
        }
    }

    public function testPMCanShareAndLinkInDefault(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $name = $this->getTestBucketName($description);
        $bucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        $defaultBranchId = $this->getDefaultBranchId($this);
        $pmBranchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        // test PM can share bucket
        $pmBranchClient->shareOrganizationBucket($bucketId);

        self::assertTrue($pmBranchClient->isSharedBucket($bucketId));
        $response = $pmBranchClient->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();
        $clientInOtherProject = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );
        // test PM can link bucket
        $linkedBucketId = $clientInOtherProject->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );

        $linkedBucket = $clientInOtherProject->getBucket($linkedBucketId);
        $this->assertSame($linkedBucketId, $linkedBucket['id']);

        $linkedBucketProjectId = $clientInOtherProject->verifyToken()['owner']['id'];
        $pmBranchClient->forceUnlinkBucket($sharedBucket['id'], $linkedBucketProjectId);

        try {
            $clientInOtherProject->getBucket($linkedBucketId);
            $this->fail('Bucket should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame(sprintf('Bucket %s not found', $linkedBucketId), $e->getMessage());
        }

        $linkedBucketId = $clientInOtherProject->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );
        $linkedBucket = $clientInOtherProject->getBucket($linkedBucketId);
        $this->assertSame($linkedBucketId, $linkedBucket['id']);

        // test PM can unlink bucket
        $clientInOtherProject->dropBucket($linkedBucketId);

        try {
            $clientInOtherProject->getBucket($linkedBucketId);
            $this->fail('Bucket should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame(sprintf('Bucket %s not found', $linkedBucketId), $e->getMessage());
        }

        // test PM can unshare bucket
        $pmBranchClient->unshareBucket($bucketId, ['async' => true]);
        $response = $pmBranchClient->listSharedBuckets();
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
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testOtherCannotShareAndLinkInMain(Client $otherRoleClient): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $pmBranchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $description = $this->generateDescriptionForTestObject();

        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);
        try {
            $otherRoleClient->shareOrganizationBucket($productionBucketId);
            $this->fail('Others should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $pmBranchClient->shareOrganizationBucket($productionBucketId);
        self::assertTrue($pmBranchClient->isSharedBucket($productionBucketId));
        $response = $pmBranchClient->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        try {
            $otherRoleClient->linkBucket(
                $linkedBucketName,
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id'],
            );
            $this->fail('Others should not be able to link bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $clientInOtherProject = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );
        $otherProjectLinkedBucketId = $clientInOtherProject->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );
        $otherProjectId = $clientInOtherProject->verifyToken()['owner']['id'];
        try {
            $otherRoleClient->forceUnlinkBucket(
                $linkedBucketName,
                $otherProjectId,
            );
            $this->fail('Others should not be able to force unlink bucket in other project');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $linkedBucketId = $pmBranchClient->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );

        try {
            $otherRoleClient->dropBucket($linkedBucketId);
            $this->fail('Others should not be able to unlink bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $pmBranchClient->dropBucket($linkedBucketId);

        try {
            $otherRoleClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Others should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    /**
     * @dataProvider tokensProvider
     */
    public function testOtherCannotShareAndLinkInBranch(Client $otherRoleClient): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $pmBranchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $description = $this->generateDescriptionForTestObject();
        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        $branch = $this->branches->createBranch($description);
        $otherRoleBranchClient = $otherRoleClient->getBranchAwareClient($branch['id']);
        try {
            $otherRoleBranchClient->shareOrganizationBucket($productionBucketId);
            $this->fail('Others should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $pmBranchClient->shareOrganizationBucket($productionBucketId);
        self::assertTrue($pmBranchClient->isSharedBucket($productionBucketId));
        $response = $pmBranchClient->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();

        // try share branch bucket
        $devBucketName = 'dev-bucket-' . sha1($this->generateDescriptionForTestObject());
        // always use developer client to create bucket in branch to work around read-only role
        $devBucketId = $this->getDeveloperStorageApiClient()
            ->getBranchAwareClient($branch['id'])
            ->createBucket(
                $devBucketName,
                'in',
            );
        try {
            $otherRoleBranchClient->shareOrganizationBucket($devBucketId);
            $this->fail('Others should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        // validate cannot link bucket from main in branch
        try {
            $otherRoleBranchClient->linkBucket(
                $linkedBucketName,
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id'],
            );
            $this->fail('Others should not be able to link bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $otherProjectClient = $this->getClientForToken(
            STORAGE_API_LINKING_TOKEN,
        );
        $linkedBucketId = $otherProjectClient->linkBucket(
            $linkedBucketName,
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
        );
        $otherProjectId = $otherProjectClient->verifyToken()['owner']['id'];
        try {
            $otherRoleBranchClient->forceUnlinkBucket(
                $sharedBucket['id'],
                $otherProjectId,
            );
            $this->fail('Others should not be able to force unlink bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
        $branchedPmClient = $pmBranchClient->getBranchAwareClient($branch['id']);
        try {
            $branchedPmClient->forceUnlinkBucket(
                $sharedBucket['id'],
                $otherProjectId,
            );
            $this->fail('PM should not be able to force unlink bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
        $otherProjectClient->dropBucket($linkedBucketId);

        // validate cannot un-share bucket from main in branch
        try {
            $otherRoleBranchClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Others should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    public function testPMCannotShareAndLinkInBranch(): void
    {
        $defaultBranchId = $this->getDefaultBranchId($this);
        $pmBranchClient = $this->getBranchAwareDefaultClient($defaultBranchId);
        $description = $this->generateDescriptionForTestObject();

        $name = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucketInDefault(self::STAGE_IN, $name, $description);

        $branch = $this->branches->createBranch($description);
        $pmBranchAwareClient = $pmBranchClient->getBranchAwareClient($branch['id']);

        try {
            $pmBranchAwareClient->shareOrganizationBucket($productionBucketId);
            $this->fail('Production manager should not be able to share bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        $pmBranchClient->shareOrganizationBucket($productionBucketId);
        self::assertTrue($pmBranchClient->isSharedBucket($productionBucketId));
        $response = $pmBranchClient->listSharedBuckets();
        self::assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedBucketName = 'linked-' . time();
        $devBucketName = 'dev-bucket-' . sha1($this->generateDescriptionForTestObject());
        // always use developer client to create bucket in branch to work around PM role limitations in dev branch
        $devBucketId = $this->getDeveloperStorageApiClient()->getBranchAwareClient($branch['id'])->createBucket(
            $devBucketName,
            'in',
        );
        try {
            $pmBranchAwareClient->shareOrganizationBucket($devBucketId);
            $this->fail('Production manager should not be able to share dev bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
        // validate cannot link bucket from main in branch
        try {
            $pmBranchAwareClient->linkBucket(
                $linkedBucketName,
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id'],
            );
            $this->fail('Production manager should not be able to link bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        // validate cannot un-share bucket from main in branch
        try {
            $pmBranchAwareClient->unshareBucket($productionBucketId, ['async' => true]);
            $this->fail('Production manager should not be able to unshare bucket in branch');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    public function initEmptyBucketInDefault(string $stage, string $name, string $description): string
    {
        $client = $this->getDefaultBranchStorageApiClient();
        return $this->initEmptyBucket($name, $stage, $description, $client);
    }
}
