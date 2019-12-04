<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;

class SharingToSpecificUsersTest extends StorageApiSharingTestCase
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
        $tokenUser = $this->_client->verifyToken()['admin'];
        $this->_client->shareBucketToUsers($bucketId, [$tokenUser['id']]);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertNotEmpty($sharedBucket['sharingParameters']);
        $this->assertInternalType('array', $sharedBucket['sharingParameters']);

        $this->_client->shareOrganizationBucket($bucketId);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertInternalType('array', $sharedBucket['sharingParameters']);

        $this->_client->shareOrganizationProjectBucket($bucketId);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertInternalType('array', $sharedBucket['sharingParameters']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketIds = $this->_bucketIds;

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken();
        $this->assertCount(0, $this->clientAdmin2InSameOrg->listSharedBuckets());

        foreach ($bucketIds as $bucketId) {
            $result = $this->_client->shareBucketToUsers(
                $bucketId,
                [$targetUser['admin']['id']]
            );

            $this->assertArrayHasKey('sharingParameters', $result);
            $this->assertArrayHasKey('users', $result['sharingParameters']);

            $this->assertCount(1, $result['sharingParameters']['users']);

            $admin = reset($result['sharingParameters']['users']);
            $this->assertEquals($targetUser['admin']['id'], $admin['id']);
            $this->assertEquals($targetUser['admin']['name'], $admin['name']);
            $this->assertEquals($targetUser['description'], $admin['email']);

            $sharedBucket = $this->_client->getBucket($bucketId);
            $this->assertArrayHasKey('sharing', $sharedBucket);
            $this->assertEquals('specific-users', $sharedBucket['sharing']);

            $this->assertArrayHasKey('sharingParameters', $sharedBucket);
            $this->assertArrayHasKey('users', $sharedBucket['sharingParameters']);

            $this->assertCount(1, $result['sharingParameters']['users']);

            $user = reset($result['sharingParameters']['users']);
            $this->assertEquals($targetUser['admin']['id'], $user['id']);
            $this->assertEquals($targetUser['admin']['name'], $user['name']);
            $this->assertEquals($targetUser['description'], $admin['email']);
        }

        $this->assertCount(2, $this->clientAdmin2InSameOrg->listSharedBuckets());
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketToUserByEmail($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->_client->verifyToken();

        $result = $this->_client->shareBucketToUsers(
            $bucketId,
            [$targetUser['description']]
        );

        $this->assertArrayHasKey('sharingParameters', $result);
        $this->assertArrayHasKey('users', $result['sharingParameters']);

        $this->assertCount(1, $result['sharingParameters']['users']);

        $admin = reset($result['sharingParameters']['users']);
        $this->assertEquals($targetUser['admin']['id'], $admin['id']);
        $this->assertEquals($targetUser['admin']['name'], $admin['name']);
        $this->assertEquals($targetUser['description'], $admin['email']);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);

        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertArrayHasKey('users', $sharedBucket['sharingParameters']);

        $this->assertCount(1, $result['sharingParameters']['users']);

        $user = reset($result['sharingParameters']['users']);
        $this->assertEquals($targetUser['admin']['id'], $user['id']);
        $this->assertEquals($targetUser['admin']['name'], $user['name']);
        $this->assertEquals($targetUser['description'], $admin['email']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testUpdateShareBucketToUser($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken()['admin'];

        $this->_client->shareBucketToUsers($bucketId, $targetUser['id']);

        $sharedBucket = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(0, $client2SharedBuckets);

        $targetUser = $this->_client2->verifyToken()['admin'];
        $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']]);

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

        $targetUser = $this->_client2->verifyToken()['admin'];
        $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']]);

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

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken()['admin'];
        $this->_client->shareBucketToUsers($bucketId, $targetUser['id']);

        $SharedBuckets = $this->clientAdmin2InSameOrg->listSharedBuckets();
        $this->assertCount(1, $SharedBuckets);

        try {
            $this->_client->linkBucket(
                "linked-" . time(),
                'in',
                $this->_client->verifyToken()['owner']['id'],
                $bucketId
            );
            $this->fail('Linking bucket by unauthorized user should fail.');
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

        $targetUser = $this->clientAdmin3InOtherOrg->verifyToken()['admin'];

        try {
            $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']]);
            $this->fail('Sharing bucket to non organization member should fail.');
        } catch (ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'Admins "[%s]" are not part of organization.',
                    $targetUser['id']
                ),
                $e->getMessage()
            );

            $this->assertEquals('storage.buckets.targetAdminsAreNotPartOfOrganization', $e->getStringCode());
            $this->assertEquals(422, $e->getCode());
        }
    }
}
