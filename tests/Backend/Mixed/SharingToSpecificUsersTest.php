<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;

class SharingToSpecificUsersTest extends StorageApiSharingTestCase
{
    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUsers = explode(',', STORAGE_API_USER_EMAIL_AVAILABLE_TO_LINK_BUCKET);

        $this->_client->shareBucketToUsers($bucketId, $targetUsers);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertArrayHasKey('users', $sharedBucket['sharingParameters']);

        foreach ($sharedBucket['sharingParameters']['users'] as $key => $sharingParameter) {
            $this->assertTrue(in_array($sharingParameter['email'], $targetUsers));
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testUpdateShareBucketToUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUsersNonAvailableForClient2 = explode(',', STORAGE_API_USER_EMAIL_AVAILABLE_TO_LINK_BUCKET);
        $targetUsers =explode(',', STORAGE_API_USER_EMAILS_NON_AVAILABLE_TO_LINK_BUCKET);

        $this->_client->shareBucketToUsers($bucketId, $targetUsersNonAvailableForClient2);

        $sharedBucket = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(0, $client2SharedBuckets);

        $this->_client->shareBucketToUsers($bucketId, $targetUsers);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $client2SharedBuckets);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkBucketToSpecificUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $emailsAvailableToClient2Link = explode(',', STORAGE_API_USER_EMAILS_NON_AVAILABLE_TO_LINK_BUCKET);
        $this->_client->shareBucketToUsers($bucketId, $emailsAvailableToClient2Link);

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
    public function testNotAbleToLinkBucketToSpecificUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetAdmins = explode(',', STORAGE_API_USER_EMAILS_NON_AVAILABLE_TO_LINK_BUCKET);

        $this->_client->shareBucketToUsers($bucketId, $targetAdmins);
        $token = $this->_client->verifyToken();

        try {
            $this->_client->linkBucket(
                "linked-" . time(),
                'in',
                $token['owner']['id'],
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
    public function testShareBucketToAnotherOrganizationUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);
        $invalidTargetAdmins = explode(',', STORAGE_API_PROJECT_IDS_NOT_IN_ORGANIZATION);

        try {
            $this->_client->shareBucketToUsers($bucketId, $invalidTargetAdmins);
            $this->fail('TargetProjectIds are not part of organization.');
        } catch (ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'Admins "[%s]" are not part of organization.',
                    implode(',', $invalidTargetAdmins)
                ),
                $e->getMessage()
            );

            $this->assertEquals('storage.buckets.targetAdminsAreNotPartOfOrganization', $e->getStringCode());
            $this->assertEquals(422, $e->getCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareOrganizationBucketChangeType($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareOrganizationBucket($bucketId);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);

        $this->_client->shareOrganizationProjectBucket($bucketId);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
    }
}