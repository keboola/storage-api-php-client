<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;

class SharingToSpecificProjectsTest extends StorageApiSharingTestCase
{
    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareOrganizationBucketChangeType($backend): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $targetProjectId = $this->clientAdmin2InSameOrg->verifyToken()['owner']['id'];
        $response = $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);

        $this->assertArrayHasKey('displayName', $response);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertNotEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);

        $response = $this->_client->shareOrganizationBucket($bucketId);

        $this->assertArrayHasKey('displayName', $response);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);

        $response = $this->_client->shareOrganizationProjectBucket($bucketId);

        $this->assertArrayHasKey('displayName', $response);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToProject($backend): void
    {
        $this->initTestBuckets($backend);
        $bucketIds = $this->_bucketIds;

        $targetProject = $this->_client->verifyToken()['owner'];
        $this->assertCount(0, $this->_client->listSharedBuckets());

        foreach ($bucketIds as $bucketId) {
            $result = $this->_client->shareBucketToProjects(
                $bucketId,
                [$targetProject['id']],
            );

            $this->assertArrayHasKey('sharingParameters', $result);
            $this->assertArrayHasKey('projects', $result['sharingParameters']);

            $this->assertCount(1, $result['sharingParameters']['projects']);

            $project = reset($result['sharingParameters']['projects']);
            $this->assertEquals($targetProject['id'], $project['id']);
            $this->assertEquals($targetProject['name'], $project['name']);
        }

        foreach ($bucketIds as $bucketId) {
            $sharedBucket = $this->_client->getBucket($bucketId);
            $this->assertArrayHasKey('sharing', $sharedBucket);
            $this->assertEquals('specific-projects', $sharedBucket['sharing']);

            $this->assertArrayHasKey('sharingParameters', $sharedBucket);
            $this->assertArrayHasKey('projects', $sharedBucket['sharingParameters']);

            $this->assertCount(1, $sharedBucket['sharingParameters']['projects']);

            $project = reset($sharedBucket['sharingParameters']['projects']);
            $this->assertEquals($targetProject['id'], $project['id']);
            $this->assertEquals($targetProject['name'], $project['name']);
        }

        $this->assertCount(2, $this->_client->listSharedBuckets());
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testUpdateShareBucketToProject($backend): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectId = $this->clientAdmin2InSameOrg->verifyToken()['owner']['id'];

        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(0, $client2SharedBuckets);

        $token = $this->_client2->verifyToken();
        $this->_client->shareBucketToProjects($bucketId, [$token['owner']['id']]);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $client2SharedBuckets);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testLinkBucketToSpecificProject($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $token = $this->_client2->verifyToken();
        $tokenAdmin2InSameOrg = $this->clientAdmin2InSameOrg->verifyToken();

        $this->_client->shareBucketToProjects(
            $bucketId,
            [
                $token['owner']['id'],
                $tokenAdmin2InSameOrg['owner']['id'],
            ],
        );

        // link
        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket(
            'linked-' . time(),
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
            null,
            $isAsync,
        );

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->_client2->getBucket($linkedBucketId);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        // link Admin2InSameOrg
        $response = $this->clientAdmin2InSameOrg->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId2 = $this->clientAdmin2InSameOrg->linkBucket(
            'linked-' . time(),
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
            null,
            $isAsync,
        );

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->clientAdmin2InSameOrg->getBucket($linkedBucketId2);

        $this->assertEquals($linkedBucketId2, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        //allow shareing only for _client token
        $this->_client->shareBucketToProjects(
            $bucketId,
            [
                $token['owner']['id'],
            ],
        );

        $response = $this->clientAdmin2InSameOrg->listSharedBuckets();
        $this->assertCount(0, $response);

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->_client2->getBucket($linkedBucketId);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->clientAdmin2InSameOrg->getBucket($linkedBucketId2);

        $this->assertEquals($linkedBucketId2, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testNotAbleToLinkBucketToSpecificProject($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectId = $this->clientAdmin2InSameOrg->verifyToken()['owner']['id'];
        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);
        $SharedBuckets = $this->clientAdmin2InSameOrg->listSharedBuckets();
        $this->assertCount(1, $SharedBuckets);

        try {
            $this->_client2->linkBucket(
                'linked-' . time(),
                'in',
                $this->_client->verifyToken()['owner']['id'],
                $bucketId,
                null,
                $isAsync,
            );
            $this->fail('Linking bucket to unauthorized project should fail.');
        } catch (ClientException $e) {
            $this->assertEquals(
                'You do not have permission to link this bucket.',
                $e->getMessage(),
            );

            $this->assertEquals('accessDenied', $e->getStringCode());
            $this->assertEquals(403, $e->getCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToAnotherOrganizationProject($backend): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectIdInOtherOrg = $this->clientAdmin3InOtherOrg->verifyToken()['owner']['id'];

        try {
            $this->_client->shareBucketToProjects($bucketId, [$targetProjectIdInOtherOrg]);
            $this->fail('Sharing bucket to project in other organization should fail.');
        } catch (ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'TargetProjectIds "[%s]" are not part of organization.',
                    $targetProjectIdInOtherOrg,
                ),
                $e->getMessage(),
            );

            $this->assertEquals('storage.buckets.targetProjectIdsAreNotPartOfOrganization', $e->getStringCode());
            $this->assertEquals(422, $e->getCode());
        }
    }
}
