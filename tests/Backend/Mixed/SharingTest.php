<?php

namespace Keboola\Test\Backend\Mixed;

use DateInterval;
use DateTimeImmutable;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\WorkspaceConnectionTrait;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Utils\EventsQueryBuilder;

class SharingTest extends StorageApiSharingTestCase
{
    use WorkspaceConnectionTrait;

    public function testOrganizationAdminInTokenVerify(): void
    {
        $token = $this->_client->verifyToken();
        $this->assertTrue($token['admin']['isOrganizationMember']);
    }

    /**
     * @dataProvider invalidSharingTypeData
     */
    public function testInvalidSharingType($sharingType, $isAsync): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        try {
            $this->_client->shareBucket(
                $bucketId,
                [
                    'sharing' => $sharingType,
                    'async' => $isAsync,
                ]
            );
            $this->fail('Bucket should not be shared');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.invalidSharingType', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    /** @dataProvider syncAsyncProvider */
    public function testTryLinkBucketWithSameNameAsAlreadyCreatedBucketThrowUserException(bool $isAsync): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketNameToShare = $this->getTestBucketName($this->generateDescriptionForTestObject()) . '-toShare';
        $this->dropBucketIfExists($this->_client2, 'out.c-' . $bucketNameToShare);

        $bucketToShare = $this->_client2->createBucket($bucketNameToShare, self::STAGE_OUT);
        $this->_client2->shareOrganizationProjectBucket($bucketToShare);

        $sharedBuckets = $this->_client->listSharedBuckets();
        $this->assertCount(1, $sharedBuckets);
        $sharedBucket = reset($sharedBuckets);
        $this->assertSame($bucketNameToShare, $sharedBucket['displayName']);
        // linking with existing name API-sharing
        try {
            // API-sharing bucket is created by initTestBuckets() for all the clients=projects
            $this->_client->linkBucket(
                self::BUCKET_API_SHARING,
                self::STAGE_IN,
                $sharedBucket['project']['id'],
                $sharedBucket['id'],
                null,
                $isAsync
            );
            $this->fail('bucket can\'t be linked with same name');
        } catch (ClientException $e) {
            $this->assertEquals(sprintf('The bucket %s already exists.', self::BUCKET_API_SHARING), $e->getMessage());
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
        }

        // linking with existing display name API-sharing. Name is used as displayName by default
        try {
            $this->_client->linkBucket(
                'not-important',
                self::STAGE_IN,
                $sharedBucket['project']['id'],
                $sharedBucket['id'],
                self::BUCKET_API_SHARING,
                $isAsync
            );
            $this->fail('bucket can\'t be linked with same display name');
        } catch (ClientException $e) {
            $this->assertEquals(sprintf('The display name "%s" already exists in project.', self::BUCKET_API_SHARING), $e->getMessage());
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
        }
    }

    /** @dataProvider syncAsyncProvider */
    public function testOrganizationPublicSharing($isAsync): void
    {
        $this->initEvents($this->_client2);

        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        /** @var array $response */
        $response = $this->_client->shareBucket($bucketId, [
            'sharing' => 'organization-project',
            'async' => $isAsync,
        ]);

        $this->assertArrayHasKey('displayName', $response);
        $this->assertEquals('organization-project', $response['sharing']);

        $token = $this->tokensInLinkingProject->createToken($this->createTestTokenOptions(true));

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $response = $client->verifyToken();
        $this->assertArrayHasKey('owner', $response);
        $this->assertArrayHasKey('id', $response['owner']);
        $this->assertArrayHasKey('name', $response['owner']);
        $linkedBucketProject = $response['owner'];
        $linkedBucketProjectId = $linkedBucketProject['id'];

        // bucket can be listed with non-admin sapi token
        $sharedBuckets = $client->listSharedBuckets();
        $this->assertCount(1, $sharedBuckets);

        $this->assertArrayHasKey('displayName', $sharedBuckets[0]);

        $this->assertEquals($bucketId, $sharedBuckets[0]['id']);
        $this->assertEquals('organization-project', $sharedBuckets[0]['sharing']);

        $displayName = 'linked-displayName';
        // bucket can be linked by another project
        $linkedBucketId = $client->linkBucket(
            'organization-project-test',
            self::STAGE_IN,
            $sharedBuckets[0]['project']['id'],
            $sharedBuckets[0]['id'],
            $displayName
        );

        $linkedBucket = $client->getBucket($linkedBucketId);
        $this->assertEquals($sharedBuckets[0]['id'], $linkedBucket['sourceBucket']['id']);
        $this->assertEquals($sharedBuckets[0]['project']['id'], $linkedBucket['sourceBucket']['project']['id']);
        $this->assertEquals($displayName, $linkedBucket['displayName']);

        // bucket can't be linked with same displayName
        try {
            $linkedBucketId = $client->linkBucket(
                'organization-project-test'. time(),
                self::STAGE_IN,
                $sharedBuckets[0]['project']['id'],
                $sharedBuckets[0]['id'],
                $displayName,
                $isAsync
            );
            $this->fail('bucket can\'t be linked with same displayName');
        } catch (ClientException $e) {
            $this->assertEquals('The display name "'.$displayName.'" already exists in project.', $e->getMessage());
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
        }

        try {
            $linkedBucketId = $client->linkBucket(
                'organization-project-test'. time(),
                self::STAGE_IN,
                $sharedBuckets[0]['project']['id'],
                $sharedBuckets[0]['id'],
                '&&&&&&',
                $isAsync
            );
            $this->fail('bucket can\'t be linked with same displayName');
        } catch (ClientException $e) {
            $this->assertEquals(
                'Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.',
                $e->getMessage()
            );
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }

        // bucket can be linked by the same project
        $selfLinkedBucketId = $this->_client->linkBucket(
            'same-project-link-test',
            self::STAGE_IN,
            $sharedBuckets[0]['project']['id'],
            $sharedBuckets[0]['id']
        );
        $selfLinkedBucket = $this->_client->getBucket($selfLinkedBucketId);
        $this->assertEquals($sharedBuckets[0]['id'], $selfLinkedBucket['sourceBucket']['id']);
        $this->assertEquals($sharedBuckets[0]['project']['id'], $selfLinkedBucket['sourceBucket']['project']['id']);

        //shared bucket should now list the linked buckets in its details
        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('linkedBy', $sharedBucket);
        $this->assertCount(2, $sharedBucket['linkedBy']);

        // verify that the listed linked buckets contains the linked bucket
        $linkedBucketKey = array_search('in.c-organization-project-test', array_column($sharedBucket['linkedBy'], 'id'));
        $this->assertNotFalse($linkedBucketKey);
        $listedLinkedBucket = $sharedBucket['linkedBy'][$linkedBucketKey];
        $this->assertArrayHasKey('project', $listedLinkedBucket);
        $this->assertEquals($linkedBucketProjectId, $listedLinkedBucket['project']['id']);
        $this->assertEquals($linkedBucketProject['name'], $listedLinkedBucket['project']['name']);
        $this->assertArrayHasKey('created', $listedLinkedBucket);
        $this->assertEquals($linkedBucket['created'], $listedLinkedBucket['created']);

        // verify the listed linked buckets includes the self-linked bucket
        $selfLinkedBucketKey = array_search('in.c-same-project-link-test', array_column($sharedBucket['linkedBy'], 'id'));
        $this->assertNotFalse($selfLinkedBucketKey);
        $listedSelfLinkedBucket = $sharedBucket['linkedBy'][$selfLinkedBucketKey];
        $this->assertArrayHasKey('project', $listedSelfLinkedBucket);
        $this->assertEquals($sharedBuckets[0]['project']['id'], $listedSelfLinkedBucket['project']['id']);
        $this->assertEquals($sharedBuckets[0]['project']['name'], $listedSelfLinkedBucket['project']['name']);
        $this->assertArrayHasKey('created', $listedSelfLinkedBucket);
        $this->assertEquals($selfLinkedBucket['created'], $listedSelfLinkedBucket['created']);

        // buckets list should include linked buckets
        $buckets = $this->_client->listBuckets(['include' => 'linkedBuckets']);
        $listedSharedBucket = (array) array_values(array_filter($buckets, function ($listBucket) use ($bucketId) {
            return ($listBucket['id'] === $bucketId);
        }))[0];

        $this->assertArrayHasKey('linkedBy', $listedSharedBucket);
        $this->assertCount(2, $listedSharedBucket['linkedBy']);

        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucketId = $bucket['linkedBy'][0]['id'];
        $linkedBucketProjectId = $bucket['linkedBy'][0]['project']['id'];

        $client->dropBucket($linkedBucketId, ['async' => $isAsync]);
        try {
            // cannot unlink bucket from nonexistent project
            $this->_client->forceUnlinkBucket($bucketId, 9223372036854775807);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame('There is no linked bucket in project "9223372036854775807"', $e->getMessage());
        }

        $notLinkedBucketName = 'normal-bucket';
        $notLinkedBucketStage = 'in';
        $notLinkedBucket = $notLinkedBucketStage . '.c-' . $notLinkedBucketName . '';
        if ($client->bucketExists($notLinkedBucket)) {
            $client->dropBucket($notLinkedBucket, ['async' => $isAsync]);
        }
        $client->createBucket($notLinkedBucketName, $notLinkedBucketStage);
        try {
            // cannot unlink bucket that is not linked from source project
            $this->_client->forceUnlinkBucket($bucketId, $linkedBucketProjectId);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf(
                    'There is no linked bucket in project "%s"',
                    $this->_client2->verifyToken()['owner']['id']
                ),
                $e->getMessage()
            );
        }

        $notSourceBucketName = 'normal-bucket';
        $notSourceBucketStage = 'in';
        $notSourceBucketId = $notSourceBucketStage . '.c-' . $notSourceBucketName . '';
        if ($this->_client->bucketExists($notSourceBucketId)) {
            $this->_client->dropBucket($notSourceBucketId, ['async' => $isAsync]);
        }
        $this->_client->createBucket($notSourceBucketName, $notSourceBucketStage);
        try {
            // cannot unlink bucket that is linked from different source bucket
            $this->_client->forceUnlinkBucket($notSourceBucketId, $linkedBucketProjectId);
            $this->fail('Should have thrown');
        } catch (ClientException $e) {
            $this->assertSame(
                'There is no linked bucket in project "' . $linkedBucketProjectId . '"',
                $e->getMessage()
            );
        }

        /** @var string $linkedBucketId */
        $linkedBucketId = $client->linkBucket(
            'organization-project-test',
            self::STAGE_IN,
            $sharedBuckets[0]['project']['id'],
            $sharedBuckets[0]['id'],
            $displayName
        );

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $this->_client->forceUnlinkBucket($bucketId, $linkedBucketProjectId);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            $this->assertSame('storage.bucketForceUnlinked', $events[0]['event']);
        };
        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketForceUnlinked')
            ->setObjectId($linkedBucketId)
            ->setObjectType('bucket')
            ->setProjectId($linkedBucketProjectId);
        $this->assertEventWithRetries($this->_client2, $assertCallback, $query, 1);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('linkedBy', $bucket);
        $this->assertCount(1, $bucket['linkedBy']);
        $this->assertFalse($client->bucketExists($linkedBucketId));

        /** @var string $linkedBucketId */
        $linkedBucketId = $client->linkBucket(
            'organization-project-test',
            self::STAGE_IN,
            $sharedBuckets[0]['project']['id'],
            $sharedBuckets[0]['id'],
            $displayName
        );

        $runId = $this->_client->generateRunId();
        $this->_client->setRunId($runId);

        $this->_client->forceUnlinkBucket($bucketId, $linkedBucketProjectId, ['async' => true]);

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            $this->assertSame('storage.bucketForceUnlinked', $events[0]['event']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketForceUnlinked')
            ->setObjectId($linkedBucketId)
            ->setObjectType('bucket')
            ->setProjectId($linkedBucketProjectId);
        $this->assertEventWithRetries($this->_client2, $assertCallback, $query, 1);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('linkedBy', $bucket);
        $this->assertCount(1, $bucket['linkedBy']);
        $this->assertFalse($client->bucketExists($linkedBucketId));

        $linkedBucketId = $client->linkBucket(
            'organization-project-test',
            self::STAGE_IN,
            $sharedBuckets[0]['project']['id'],
            $sharedBuckets[0]['id'],
            $displayName
        );

        // bucket unlink with token without canManage permission
        $token = $this->tokensInLinkingProject->createToken($this->createTestTokenOptions(false));

        $this->tokensInLinkingProject->updateToken(
            (new TokenUpdateOptions($token['id']))
                ->addBucketPermission($linkedBucketId, TokenAbstractOptions::BUCKET_PERMISSION_READ)
        );

        $cannotManageBucketsClient = $this->getClientForToken($token['token']);

        $this->assertTrue($cannotManageBucketsClient->bucketExists($linkedBucketId));

        try {
            $cannotManageBucketsClient->dropBucket($linkedBucketId, ['async' => $isAsync]);
            $this->fail('Bucket unlink should be restricted for tokens without canManageBuckets permission');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        $this->assertTrue($this->_client2->bucketExists($linkedBucketId));

        // user should be also able to delete the linked bucket
        $client->dropBucket($linkedBucketId, ['async' => $isAsync]);

        $this->assertFalse($this->_client2->bucketExists($linkedBucketId));
    }

    /** @dataProvider syncAsyncProvider */
    public function testNonOrganizationAdminInToken($isAsync): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);
        $linkedId = $this->_client->linkBucket(
            'linked-' . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // new token creation
        $token = $this->tokens->createToken($this->createTestTokenOptions(true));

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $client->verifyToken();

        $this->assertEmpty($client->listSharedBuckets());

        try {
            $client->shareBucket($bucketId, ['async' => $isAsync]);
            $this->fail('`shareBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->unshareBucket($bucketId, $isAsync);
            $this->fail('`unshareBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->linkBucket(
                'linked-' . time(),
                'out',
                $sharedBucket['project']['id'],
                $sharedBucket['id']
            );
            $this->fail('`linkBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->dropBucket($linkedId, ['async' => $isAsync]);
            $this->fail('`dropBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareBucket($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $this->_client->unshareBucket($bucketId);
        $this->assertFalse($this->_client->isSharedBucket($bucketId));

        // sharing twice
        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

        try {
            $this->_client->shareBucket($bucketId, ['async' => $isAsync]);
            $this->fail('sharing twice should fail');
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.shareTwice', $e->getStringCode());
        }
    }

    /**
     * @dataProvider syncAsyncProvider
     * @throws ClientException
     */
    public function testAdminWithShareRoleSharesBucket($isAsync): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $tokenData = $this->shareRoleClient->verifyToken();
        $this->assertSame('share', $tokenData['admin']['role']);

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken();

        /** @var array $bucket */
        $bucket = $this->shareRoleClient->shareBucket($bucketId, ['async' => $isAsync]);
        $this->assertSame('organization', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareOrganizationProjectBucket($bucketId, $isAsync);
        $this->assertSame('organization-project', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareBucketToUsers($bucketId, [
            $targetUser['admin']['id'],
        ], $isAsync);
        $this->assertSame('specific-users', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareBucketToProjects($bucketId, [
            $targetUser['owner']['id'],
        ], $isAsync);
        $this->assertSame('specific-projects', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareOrganizationBucket($bucketId);
        $this->assertSame('organization', $bucket['sharing']);

        $this->shareRoleClient->unshareBucket($bucketId, $isAsync);
        $this->assertFalse($this->shareRoleClient->isSharedBucket($bucketId));
    }

    /**
     * @dataProvider syncAsyncProvider
     * @throws ClientException
     */
    public function testAdminWithShareRoleSharesBucketAsQuery(bool $isAsync): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $tokenData = $this->shareRoleClient->verifyToken();
        $this->assertSame('share', $tokenData['admin']['role']);

        $targetUser = $this->clientAdmin2InSameOrg->verifyToken();

        /** @var array $bucket */
        $bucket = $this->shareRoleClient->shareBucket($bucketId, ['async' => $isAsync]);
        $this->assertSame('organization', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareOrganizationProjectBucket($bucketId, $isAsync);
        $this->assertSame('organization-project', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareBucketToUsersAsQuery($bucketId, [
            $targetUser['admin']['id'],
        ], $isAsync);
        $this->assertSame('specific-users', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareBucketToProjectsAsQuery($bucketId, [
            $targetUser['owner']['id'],
        ], $isAsync);
        $this->assertSame('specific-projects', $bucket['sharing']);

        $bucket = $this->shareRoleClient->shareOrganizationBucket($bucketId);
        $this->assertSame('organization', $bucket['sharing']);

        $this->shareRoleClient->unshareBucket($bucketId, $isAsync);
        $this->assertFalse($this->shareRoleClient->isSharedBucket($bucketId));
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareBucketChangeType($backend, $isASync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $targetProjectId = $this->clientAdmin2InSameOrg->verifyToken()['owner']['id'];
        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId], $isASync);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertNotEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);

        // first reshare
        $this->_client->changeBucketSharing($bucketId, 'organization-project', $isASync);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);

        // second reshare
        $this->_client->changeBucketSharing($bucketId, 'organization', $isASync);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertIsArray($sharedBucket['sharingParameters']);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testShareBucketChangeTypeOnUnsharedBucket($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        try {
            $this->_client->changeBucketSharing($bucketId, 'organization-project', $isAsync);
            $this->fail('change of sharing type of non-shared bucket should\'nt be possible');
        } catch (ClientException $e) {
            $this->assertEquals('The bucket out.c-API-sharing is not shared.', $e->getMessage());
            $this->assertEquals('storage.bucket.notShared', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testSharedBuckets($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $expectedTableName = 'numbers';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            $expectedTableName,
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $expectedTableAliasName = 'numbers_alias';
        $aliasTableId = $this->_client->createAliasTable(
            $bucketId,
            $tableId,
            $expectedTableAliasName
        );

        $this->_client->createAliasTable(
            $bucketId,
            $tableId,
            'numbers_filtered_alias',
            [
                'aliasFilter' => [
                    'column' => '0',
                    'values' => ['PRG'],
                    'operator' => 'eq',
                ],
            ]
        );

        // ensure that sharing data is not output for unshared bucket
        $this->assertFalse($this->_client->isSharedBucket($bucketId));
        $bucketBeforeSharing = $this->_client->getBucket($bucketId);
        $this->assertArrayNotHasKey('sharedBy', $bucketBeforeSharing);
        $this->assertArrayNotHasKey('sharingParameters', $bucketBeforeSharing);

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $verifyTokenResponse = $this->_client->verifyToken();
        $this->assertArrayHasKey('owner', $verifyTokenResponse);

        $this->assertArrayHasKey('id', $verifyTokenResponse['owner']);
        $this->assertArrayHasKey('name', $verifyTokenResponse['owner']);

        $project = $verifyTokenResponse['owner'];

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = $response[0];
        $this->assertArrayHasKey('id', $sharedBucket);
        $this->assertArrayHasKey('description', $sharedBucket);
        $this->assertArrayHasKey('project', $sharedBucket);
        $this->assertArrayHasKey('tables', $sharedBucket);
        $this->assertArrayHasKey('created', $sharedBucket);
        $this->assertArrayHasKey('lastChangeDate', $sharedBucket);
        $this->assertArrayHasKey('dataSizeBytes', $sharedBucket);
        $this->assertArrayHasKey('rowsCount', $sharedBucket);
        $this->assertArrayHasKey('backend', $sharedBucket);

        $this->assertArrayHasKey('id', $sharedBucket['project']);
        $this->assertArrayHasKey('name', $sharedBucket['project']);

        $this->assertEquals($sharedBucket['project']['id'], $project['id']);
        $this->assertEquals($sharedBucket['project']['name'], $project['name']);

        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertSame([], $sharedBucket['sharingParameters']);

        $this->assertArrayHasKey('id', $sharedBucket['sharedBy']);
        $this->assertArrayHasKey('name', $sharedBucket['sharedBy']);

        $this->assertEquals(
            $verifyTokenResponse['id'],
            $sharedBucket['sharedBy']['id']
        );
        $this->assertEquals(
            $verifyTokenResponse['description'],
            $sharedBucket['sharedBy']['name']
        );
        $this->assertNotNull(
            $sharedBucket['sharedBy']['date']
        );
        $this->assertGreaterThan(
            (new DateTimeImmutable())->sub(new DateInterval('PT5M')),
            new DateTimeImmutable($sharedBucket['sharedBy']['date'])
        );

        // should show table and alias, but not filtered alias
        $this->assertCount(2, $sharedBucket['tables']);

        $sharedTable = array_values(
            array_filter($sharedBucket['tables'], static function (array $table) use ($tableId) {
                return $table['id'] === $tableId;
            })
        );
        $this->assertCount(1, $sharedTable);
        $sharedTable = $sharedTable[0];
        $this->assertSharedTable($sharedTable, $expectedTableName);

        $sharedTableAlias = array_values(
            array_filter($sharedBucket['tables'], static function (array $table) use ($aliasTableId) {
                return $table['id'] === $aliasTableId;
            })
        );
        $this->assertCount(1, $sharedTableAlias);
        $sharedTableAlias = $sharedTableAlias[0];
        $this->assertSharedTable($sharedTableAlias, $expectedTableAliasName);
    }

    /**
     * @param array $sharedTable
     * @param string $tableName
     * @return void
     */
    private function assertSharedTable($sharedTable, $tableName)
    {
        $this->assertArrayHasKey('id', $sharedTable);
        $this->assertArrayHasKey('name', $sharedTable);

        $this->assertEquals($tableName, $sharedTable['name']);
        $this->assertEquals(
            $tableName,
            $sharedTable['displayName'],
            'display name is same as name'
        );
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testSharedBucketsWithInclude($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $tableName = 'numbers';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            $tableName,
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $metadataClient = new Metadata($this->_client);
        $metadataClient->postBucketMetadata(
            $bucketId,
            'test',
            [
                [
                    'key' => 'test.metadata.key',
                    'value' => 'test.metadata.value',
                ],
            ]
        );

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client->verifyToken();
        $this->assertArrayHasKey('owner', $response);

        $this->assertArrayHasKey('id', $response['owner']);
        $this->assertArrayHasKey('name', $response['owner']);

        $project = $response['owner'];

        $response = $this->_client2->listSharedBuckets(['include' => 'metadata']);
        $this->assertCount(1, $response);

        foreach ($response as $sharedBucket) {
            $this->assertArrayHasKey('id', $sharedBucket);
            $this->assertArrayHasKey('description', $sharedBucket);
            $this->assertArrayHasKey('project', $sharedBucket);
            $this->assertArrayHasKey('tables', $sharedBucket);

            $this->assertArrayHasKey('id', $sharedBucket['project']);
            $this->assertArrayHasKey('name', $sharedBucket['project']);

            $this->assertEquals($sharedBucket['project']['id'], $project['id']);
            $this->assertEquals($sharedBucket['project']['name'], $project['name']);

            $this->assertCount(1, $sharedBucket['tables']);

            $sharedBucketTable = reset($sharedBucket['tables']);

            $this->assertArrayHasKey('id', $sharedBucketTable);
            $this->assertArrayHasKey('name', $sharedBucketTable);

            $this->assertEquals($tableId, $sharedBucketTable['id']);
            $this->assertEquals($tableName, $sharedBucketTable['name']);

            $this->assertCount(1, $sharedBucket['metadata']);

            $sharedBucketMetadata = reset($sharedBucket['metadata']);

            $this->assertArrayHasKey('id', $sharedBucketMetadata);
            $this->assertArrayHasKey('key', $sharedBucketMetadata);
            $this->assertArrayHasKey('value', $sharedBucketMetadata);
            $this->assertArrayHasKey('provider', $sharedBucketMetadata);
            $this->assertArrayHasKey('timestamp', $sharedBucketMetadata);

            $this->assertEquals('test', $sharedBucketMetadata['provider']);
            $this->assertEquals('test.metadata.key', $sharedBucketMetadata['key']);
            $this->assertEquals('test.metadata.value', $sharedBucketMetadata['value']);
        }
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testLinkBucketDry($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);
        $sourceBucket = $this->_client->getBucket($bucketId);

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket('linked-' . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id'], null, $isAsync);

        $bucket = $this->_client2->getBucket($id);

        $this->assertArrayHasKey('id', $bucket);
        $this->assertArrayHasKey('stage', $bucket);
        $this->assertArrayHasKey('backend', $bucket);
        $this->assertArrayHasKey('description', $bucket);
        $this->assertArrayHasKey('isReadOnly', $bucket);

        $this->assertEquals($id, $bucket['id']);
        $this->assertEquals('out', $bucket['stage']);
        $this->assertTrue($bucket['isReadOnly']);
        $this->assertEquals($sourceBucket['backend'], $bucket['backend']);
        $this->assertEquals($sourceBucket['description'], $bucket['description']);

        // source bucket should list linked bucket in detail
        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('linkedBy', $sharedBucket);
        $this->assertCount(1, $sharedBucket['linkedBy']);
        $this->assertArrayHasKey('project', $sharedBucket['linkedBy'][0]);
        $this->assertArrayHasKey('created', $sharedBucket['linkedBy'][0]);
        $this->assertEquals($bucket['created'], $sharedBucket['linkedBy'][0]['created']);
        $this->assertArrayHasKey('id', $sharedBucket['linkedBy'][0]);
        $this->assertEquals($id, $sharedBucket['linkedBy'][0]['id']);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testLinkBucketToOrganizationDeletePermissions($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);
        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket('linked-' . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id'], null, $isAsync);

        $token = $this->tokensInLinkingProject->createToken($this->createTestTokenOptions(true));

        $client = $this->getClient([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        try {
            $client->dropBucket($linkedBucketId, ['async' => $isAsync]);
            $this->fail('non-organization member should not be able to delete bucket');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
            $this->assertEquals(403, $e->getCode());
        }

        // organization member should be able to delete linked bucket
        $this->_client2->dropBucket($linkedBucketId, ['async' => $isAsync]);
    }

    /** @dataProvider syncAsyncProvider */
    public function testBucketCannotBeLinkedMoreTimes($isAsync): void
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId, ['async'=>$isAsync]);

        $response = $this->_client2->listSharedBuckets();
        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket('linked-' . uniqid(), 'out', $sharedBucket['project']['id'], $sharedBucket['id'], null, $isAsync);
        try {
            $this->_client2->linkBucket('linked-' . uniqid(), 'out', $sharedBucket['project']['id'], $sharedBucket['id'], null, $isAsync);
            $this->fail('bucket should not be linked');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
        }
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testLinkedBucket(string $backend, bool $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $displayName = 'display-name-first';
        $this->_client->updateTable($tableId, ['displayName' => $displayName]);

        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_OUT),
            'languages-out',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $metadataApi = new Metadata($this->_client);
        $testMetadata = [
            [
                'key' => 'test_metadata_key1',
                'value' => 'testval',
            ],
            [
                'key' => 'test_metadata_key2',
                'value' => 'testval',
            ],
        ];

        $columnId = $table2Id . '.id';
        $expectedMetadata = $metadataApi->postColumnMetadata($columnId, self::TEST_METADATA_PROVIDER, $testMetadata);

        $aliasTableId = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'languages-alias'
        );

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

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
            $isAsync
        );

        // validate bucket
        $bucket = $this->_client->getBucket($bucketId);
        $linkedBucket = $this->_client2->getBucket($linkedBucketId);

        $this->assertEquals($linkedBucketId, $linkedBucket['id']);
        $this->assertEquals('in', $linkedBucket['stage']);
        $this->assertEquals($bucket['backend'], $linkedBucket['backend']);
        $this->assertEquals($bucket['description'], $linkedBucket['description']);

        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // new import
        $this->_client->writeTableAsync(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
            [
                'primaryKey' => 'id',
                'incremental' => true,
            ]
        );

        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // remove primary key
        $this->_client->removeTablePrimaryKey($tableId);
        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // add primary key
        $this->_client->createTablePrimaryKey($tableId, ['id', 'name']);
        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // add column
        $this->_client->addTableColumn($tableId, 'fake');
        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // delete rows
        $this->_client->deleteTableRows($tableId, [
            'whereColumn' => 'id',
            'whereValues' => ['new'],
        ]);
        $this->assertTablesMetadata($bucketId, $linkedBucketId);

        // aditional table
        $this->_client->createTableAsync(
            $bucketId,
            'second',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );
        $aliasId = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'languages-alias-2'
        );
        $this->assertTablesMetadata($bucketId, $linkedBucketId);
        $aliasTable = $this->_client->getTable($aliasId, ['include' => 'columnMetadata']);
        $this->assertSame($expectedMetadata, $aliasTable['sourceTable']['columnMetadata']['id']);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testRestrictedDrop($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

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
            $isAsync
        );

        $tables = $this->_client->listTables($bucketId);
        $this->assertCount(1, $tables);

        // table drop
        foreach ($this->_client->listTables($bucketId) as $table) {
            try {
                $this->_client->dropTable($table['id']);
                $this->fail('Shared table delete should fail');
            } catch (ClientException $e) {
                $this->assertEquals('tables.cannotDeletedTableWithAliases', $e->getStringCode());
            }

            try {
                $this->_client->deleteTableColumn($table['id'], 'name');
                $this->fail('Shared table column delete should fail');
            } catch (ClientException $e) {
                $this->assertEquals('storage.tables.cannotDeleteReferencedColumn', $e->getStringCode());
            }
        }

        // bucket drop
        try {
            $this->_client->dropBucket($bucketId, ['async' => $isAsync]);
            $this->fail('Shared bucket delete should fail');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
        }

        $this->assertTablesMetadata($bucketId, $linkedBucketId);
    }

    /**
     * @dataProvider sharingBackendDataWithAsync
     * @throws ClientException
     */
    public function testForcedDrop($backend, $isAsync): void
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // prepare bucket tables
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            'first',
            new CsvFile(__DIR__ . '/../../_data/pk.simple.csv'),
            [
                'primaryKey' => 'id',
            ]
        );

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

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
            $isAsync
        );

        $tables = $this->_client->listTables($bucketId);
        $this->assertCount(1, $tables);

        foreach ($this->_client->listTables($bucketId) as $table) {
            // column drop
            $this->_client->deleteTableColumn($table['id'], 'name', ['force' =>  true]);

            $detail = $this->_client->getTable($table['id']);
            $this->assertEquals(['id'], $detail['columns']);

            $this->assertTablesMetadata($bucketId, $linkedBucketId);

            // table drop
            $this->_client->dropTable($table['id'], ['force' =>  true]);
        }

        $this->assertCount(0, $this->_client->listTables($bucketId));
        $this->assertCount(0, $this->_client2->listTables($linkedBucketId));
    }

    /**
     *
     *
     * @dataProvider workspaceMixedBackendData
     * @throws ClientException
     * @throws \Exception
     * @throws \Keboola\StorageApi\Exception
     */
    public function testWorkspaceLoadData($sharingBackend, $workspaceBackend, $isAsync): void
    {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets($sharingBackend);
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $secondBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $table1Id = $this->_client->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2Id = $this->_client->createTableAsync(
            $bucketId,
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $table3Id = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'numbers-alias'
        );

        // share and link bucket
        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedId = $this->_client2->linkBucket(
            'linked-' . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id'],
            null,
            $isAsync
        );

        // share and unshare second bucket - test that it doesn't break permissions of first linked bucket
        $this->_client->shareBucket($secondBucketId, ['async' => $isAsync]);
        $sharedBucket2 = array_values(array_filter($this->_client->listSharedBuckets(), function ($bucket) use ($secondBucketId) {
            return $bucket['id'] === $secondBucketId;
        }))[0];
        $linked2Id = $this->_client2->linkBucket(
            'linked-2-' . time(),
            'out',
            $sharedBucket2['project']['id'],
            $sharedBucket2['id'],
            null,
            $isAsync
        );
        $this->_client2->dropBucket($linked2Id, ['async' => $isAsync]);

        $mapping1 = [
            'source' => str_replace($bucketId, $linkedId, $table1Id),
            'destination' => 'languagesLoaded',
        ];

        $mapping2 = [
            'source' => str_replace($bucketId, $linkedId, $table2Id),
            'destination' => 'numbersLoaded',
        ];

        $mapping3 = [
            'source' => str_replace($bucketId, $linkedId, $table3Id),
            'destination' => 'numbersAliasLoaded',
        ];

        // init workspace
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $workspaceBackend,
            ],
            true
        );

        $input = [$mapping1, $mapping2, $mapping3];

        // test if job is created and listed
        $initialJobs = $this->_client2->listJobs();
        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => $input]);
        $afterJobs = $this->_client2->listJobs();

        $this->assertEquals('workspaceLoad', $afterJobs[0]['operationName']);
        $this->assertNotEquals(empty($initialJobs) ? 0 : $initialJobs[0]['id'], $afterJobs[0]['id']);

        // block until async events are processed, processing in order is not guaranteed but it should work most of time
        $this->createAndWaitForEvent((new \Keboola\StorageApi\Event())->setComponent('dummy')->setMessage('dummy'));

        $stats = $this->_client2->getStats((new \Keboola\StorageApi\Options\StatsOptions())->setRunId($runId));

        $export = $stats['tables']['export'];
        $this->assertEquals(3, $export['totalCount']);
        $this->assertCount(3, $export['tables']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $tables = $backend->getTables();

        // check that the tables are in the workspace
        $this->assertCount(3, $tables);
        $this->assertContains($backend->toIdentifier('languagesLoaded'), $tables);
        $this->assertContains($backend->toIdentifier('numbersLoaded'), $tables);
        $this->assertContains($backend->toIdentifier('numbersAliasLoaded'), $tables);

        // check table structure and data
        $data = $backend->fetchAll('languagesLoaded', \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ',', '"'),
            $data,
            'id'
        );

        // now we'll load another table and use the preserve parameters to check that all tables are present
        // lets create it now to see if the table permissions are correctly propagated
        $table3Id = $this->_client->createTableAsync(
            $bucketId,
            'numbersLater',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $mapping3 = ['source' => str_replace($bucketId, $linkedId, $table3Id), 'destination' => 'table3'];
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping3], 'preserve' => true]);

        $tables = $backend->getTables();

        $this->assertCount(4, $tables);
        $this->assertContains($backend->toIdentifier('table3'), $tables);
        $this->assertContains($backend->toIdentifier('languagesLoaded'), $tables);
        $this->assertContains($backend->toIdentifier('numbersLoaded'), $tables);
        $this->assertContains($backend->toIdentifier('numbersAliasLoaded'), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], ['input' => [$mapping3]]);

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier('table3'), $tables);

        // unload validation
        $connection = $workspace['connection'];

        $backend = null; // force disconnect of same SNFLK connection
        $db = $this->getDbConnection($connection);

        $db->query('create table "test.Languages3" (
			"Id" integer not null,
			"Name" varchar not null
		);');
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        try {
            $this->_client2->createTableAsyncDirect($linkedId, [
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages3',
            ]);

            $this->fail('Unload to liked bucket should fail with access exception');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /** @dataProvider syncAsyncProvider */
    public function testCloneLinkedBucket($isAsync): void
    {
        $this->deleteAllWorkspaces();
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);

        // prepare source data
        $sourceBucketId = $this->getTestBucketId();
        $table1Id = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languagesDetails',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $this->_client->shareBucket($sourceBucketId, ['async'=>$isAsync]);

        $table2Id = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $table3Id = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_OUT),
            'languages-out',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table4Id = $this->_client->createAliasTable(
            $sourceBucketId,
            $table3Id,
            'languages-alias'
        );

        $sourceProjectId = $this->_client->verifyToken()['owner']['id'];
        $linkedId = $this->_client2->linkBucket(
            'linked-' . uniqid(),
            'out',
            $sourceProjectId,
            $sourceBucketId,
            null,
            $isAsync
        );

        // load data into workspace in destination project
        $workspacesClient = new Workspaces($this->_client2);
        $workspace = $workspacesClient->createWorkspace(
            [
                'backend' => self::BACKEND_SNOWFLAKE,
            ],
            true
        );

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => str_replace($sourceBucketId, $linkedId, $table1Id),
                    'destination' => 'languagesDetails',
                ],
                [
                    'source' => str_replace($sourceBucketId, $linkedId, $table2Id),
                    'destination' => 'NUMBERS',
                ],
                [
                    'source' => str_replace($sourceBucketId, $linkedId, $table4Id),
                    'destination' => 'languagesAlias',
                ],
            ],
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        // assert table 1 data
        $workspaceTableColumns = $backend->describeTableColumns('languagesDetails');
        $this->assertEquals(
            [
                [
                    'name' => 'id',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => 'name',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '_timestamp',
                    'type' => 'TIMESTAMP_NTZ(9)',
                ],
            ],
            array_map(
                function (array $column) {
                    return [
                        'name' => $column['name'],
                        'type' => $column['type'],
                    ];
                },
                $workspaceTableColumns
            )
        );

        $workspaceTableData = $backend->fetchAll('languagesDetails');
        $this->assertCount(5, $workspaceTableData);

        // assert table 2 data
        $workspaceTableColumns = $backend->describeTableColumns('NUMBERS');
        $this->assertEquals(
            [
                [
                    'name' => '0',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '1',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '2',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '3',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '45',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '_timestamp',
                    'type' => 'TIMESTAMP_NTZ(9)',
                ],
            ],
            array_map(
                function (array $column) {
                    return [
                        'name' => $column['name'],
                        'type' => $column['type'],
                    ];
                },
                $workspaceTableColumns
            )
        );

        $workspaceTableData = $backend->fetchAll('NUMBERS');
        $this->assertCount(1, $workspaceTableData);

        // assert alias table  data
        $workspaceTableColumns = $backend->describeTableColumns('languagesAlias');
        $this->assertEquals(
            [
                [
                    'name' => 'id',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => 'name',
                    'type' => 'VARCHAR(16777216)',
                ],
                [
                    'name' => '_timestamp',
                    'type' => 'TIMESTAMP_NTZ(9)',
                ],
            ],
            array_map(
                function (array $column) {
                    return [
                        'name' => $column['name'],
                        'type' => $column['type'],
                    ];
                },
                $workspaceTableColumns
            )
        );

        $workspaceTableData = $backend->fetchAll('languagesAlias');
        $this->assertCount(5, $workspaceTableData);
    }

    public function invalidSharingTypeData()
    {
        foreach ([true, false] as $async) {
            yield sprintf('non existing type with async=%b', $async) => [
                'global',
                $async,
            ];

            yield sprintf('sharing to specific projects with async=%b', $async) => [
                'specific-projects',
                $async,
            ];

            yield sprintf('sharing to specific users with async=%b', $async) => [
                'specific-users',
                $async,
            ];
        }
    }

    public function syncAsyncProvider()
    {
        yield 'sync call action' => [
            false,
        ];

        yield 'async call action' => [
            true,
        ];
    }

    /** @dataProvider syncAsyncProvider */
    public function testDevBranchBucketCannotBeShared($isAsync): void
    {
        $metadataProvider = Metadata::PROVIDER_SYSTEM;
        $metadataKey = Metadata::BUCKET_METADATA_KEY_ID_BRANCH;

        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $tableName = 'languages';
        $tableId = $this->_client->createTableAsync(
            $bucketId,
            $tableName,
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $metadata = new Metadata($this->_client);

        // check that validation ignores table/columns metadata
        $metadata->postColumnMetadata(
            sprintf('%s.%s', $tableId, 'id'),
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => '1234',
                ],
            ]
        );

        $metadata->postTableMetadata(
            $tableId,
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => '1234',
                ],
            ]
        );

        $this->_client->shareBucket($bucketId, ['async' => $isAsync]);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertSame('organization', $bucket['sharing']);

        $this->_client->unshareBucket($bucketId, $isAsync);

        // validate restrictions
        $metadata->postBucketMetadata(
            $bucketId,
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => '1234',
                ],
            ]
        );

        try {
            $this->_client->shareBucket($bucketId, ['async' => $isAsync]);
            $this->fail('Sharing buckets from Dev/Branch should fail');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('Sharing Dev/Branch buckets is not supported yet.', $e->getMessage());
        }

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEmpty($bucket['sharing']);
    }

    private function createTestTokenOptions($canManageBuckets)
    {
        return (new TokenCreateOptions())
            ->setDescription('Test Token')
            ->setCanManageBuckets($canManageBuckets)
            ->setExpiresIn(3600)
        ;
    }
}
