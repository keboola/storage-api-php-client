<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;

class SharingToSpecificProjectsTest extends StorageApiSharingTestCase
{
    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareOrganizationBucketChangeType($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $targetProjectId = $this->clientInSameOrg->verifyToken()['owner']['id'];
        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);

        $this->_client->shareOrganizationBucket($bucketId);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);
        $this->assertArrayNotHasKey('sharingParameters', $sharedBucket);

        $this->_client->shareOrganizationProjectBucket($bucketId);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
        $this->assertArrayNotHasKey('sharingParameters', $sharedBucket);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToProject($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectId = $this->clientInSameOrg->verifyToken()['owner']['id'];
        $result = $this->_client->shareBucketToProjects(
            $bucketId,
            [$targetProjectId]
        );

        $this->assertArrayHasKey('sharingParameters', $result);
        $this->assertArrayHasKey('projects', $result['sharingParameters']);
        foreach ($result['sharingParameters']['projects'] as $key => $sharingParameter) {
            $this->assertTrue(in_array($sharingParameter['id'], [$targetProjectId]));
        }

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertArrayHasKey('projects', $sharedBucket['sharingParameters']);

        foreach ($sharedBucket['sharingParameters']['projects'] as $key => $sharingParameter) {
            $this->assertTrue(in_array($sharingParameter['id'], [$targetProjectId]));
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testUpdateShareBucketToProject($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectId = $this->clientInSameOrg->verifyToken()['owner']['id'];

        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(0, $client2SharedBuckets);

        $token = $this->_client2->verifyToken();
        $this->_client->shareBucketToProjects($bucketId, $token['owner']['id']);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $client2SharedBuckets);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkBucketToSpecificProject($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $token = $this->_client2->verifyToken();
        $this->_client->shareBucketToProjects($bucketId, $token['owner']['id']);

        // link
        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket(
            "linked-" . time(),
            'in',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->_client2->getBucket($linkedBucketId);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testNotAbleToLinkBucketToSpecificProject($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectId = $this->clientInSameOrg->verifyToken()['owner']['id'];
        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);
        $SharedBuckets = $this->clientInSameOrg->listSharedBuckets();
        $this->assertCount(1, $SharedBuckets);

        try {
            $this->_client2->linkBucket(
                "linked-" . time(),
                'in',
                $this->_client->verifyToken()['owner']['id'],
                $bucketId
            );
            $this->fail('You do not have permission to link this bucket.');
        } catch (ClientException $e) {
            $this->assertEquals(
                'You do not have permission to link this bucket.',
                $e->getMessage()
            );

            $this->assertEquals('accessDenied', $e->getStringCode());
            $this->assertEquals(403, $e->getCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToAnotherOrganizationProject($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetProjectIdInOtherOrg = $this->clientInOtherOrg->verifyToken()['owner']['id'];

        try {
            $this->_client->shareBucketToProjects($bucketId, [$targetProjectIdInOtherOrg]);
            $this->fail('TargetProjectIds are not part of organization.');
        } catch (ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'TargetProjectIds "[%s]" are not part of organization.',
                    $targetProjectIdInOtherOrg
                ),
                $e->getMessage()
            );

            $this->assertEquals('storage.buckets.targetProjectIdsAreNotPartOfOrganization', $e->getStringCode());
            $this->assertEquals(422, $e->getCode());
        }
    }
}
