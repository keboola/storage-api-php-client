<?php

namespace Keboola\Test\Backend\Mixed;

use Keboola\StorageApi\ClientException;

class SharingToSpecificUsersTest extends StorageApiSharingTestCase
{
    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareOrganizationBucketChangeType($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $tokenUser = $this->_client->verifyToken()['admin'];
        $response = $this->_client->shareBucketToUsers($bucketId, [$tokenUser['id']], $isAsync);

        $this->assertArrayHasKey('displayName', $response);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertNotEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);

        $this->_client->shareOrganizationBucket($bucketId, $isAsync);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);

        $this->_client->shareOrganizationProjectBucket($bucketId, $isAsync);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareBucketToUser($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketIds = $this->_bucketIds;

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken();
        $this->assertCount(0, $this->clientAdmin2InSameOrg->listSharedBuckets());

        foreach ($bucketIds as $bucketId) {
            $result = $this->_client->shareBucketToUsers(
                $bucketId,
                [$targetUser['admin']['id']],
                $isAsync,
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
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareBucketToUserByEmail($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->_client->verifyToken();

        $result = $this->_client->shareBucketToUsers(
            $bucketId,
            [$targetUser['description']],
            $isAsync,
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
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testUpdateShareBucketToUser($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken()['admin'];

        $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']], $isAsync);

        $sharedBucket = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-users', $sharedBucket['sharing']);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(0, $client2SharedBuckets);

        $targetUser = $this->_client2->verifyToken()['admin'];
        $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']], $isAsync);

        $client2SharedBuckets = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $client2SharedBuckets);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testLinkBucketToSpecificUser($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->_client2->verifyToken()['admin'];
        $targetAdmin2InSameOrg = $this->clientAdmin2InSameOrg->verifyToken()['admin'];
        $this->_client->shareBucketToUsers(
            $bucketId,
            [
                $targetUser['id'],
                $targetAdmin2InSameOrg['id'],
            ],
            $isAsync,
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

        // link
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
        $linkedBucket2 = $this->clientAdmin2InSameOrg->getBucket($linkedBucketId2);

        $this->assertEquals($linkedBucketId2, $linkedBucket2['id']);
        $this->assertEquals('in', $linkedBucket2['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket2['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket2['description']);

        //allow shareing only for _client token
        $this->_client->shareBucketToUsers(
            $bucketId,
            [
                $targetUser['id'],
            ],
            $isAsync,
        );

        $response = $this->clientAdmin2InSameOrg->listSharedBuckets();
        $this->assertCount(0, $response);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        $this->assertEquals($linkedBucketId2, $linkedBucket2['id']);
        $this->assertEquals('in', $linkedBucket2['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket2['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket2['description']);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testNotAbleToLinkBucketToSpecificUser($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken()['admin'];
        $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']], $isAsync);

        $SharedBuckets = $this->clientAdmin2InSameOrg->listSharedBuckets();
        $this->assertCount(1, $SharedBuckets);

        try {
            $this->_client->linkBucket(
                'linked-' . time(),
                'in',
                $this->_client->verifyToken()['owner']['id'],
                $bucketId,
                null,
                $isAsync,
            );
            $this->fail('Linking bucket by unauthorized user should fail.');
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
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareBucketToAnotherOrganizationUser($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $targetUser = $this->clientAdmin3InOtherOrg->verifyToken()['admin'];

        try {
            $this->_client->shareBucketToUsers($bucketId, [$targetUser['id']], $isAsync);
            $this->fail('Sharing bucket to non organization member should fail.');
        } catch (ClientException $e) {
            $this->assertEquals(
                sprintf(
                    'Admins "[%s]" are not part of organization.',
                    $targetUser['id'],
                ),
                $e->getMessage(),
            );

            $this->assertEquals('storage.buckets.targetAdminsAreNotPartOfOrganization', $e->getStringCode());
            $this->assertEquals(422, $e->getCode());
        }
    }
}
