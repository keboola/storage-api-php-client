<?php

namespace Keboola\Test\Backend\Mixed;

use DateInterval;
use DateTimeImmutable;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SharingTest extends StorageApiSharingTestCase
{
    public function testOrganizationAdminInTokenVerify()
    {
        $token = $this->_client->verifyToken();
        $this->assertTrue($token['admin']['isOrganizationMember']);
    }

    /**
     * @dataProvider invalidSharingTypeData
     */
    public function testInvalidSharingType($sharingType)
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        try {
            $this->_client->shareBucket($bucketId, [
                'sharing' => $sharingType,
            ]);
            $this->fail('Bucket should not be shared');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.invalidSharingType', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    public function testOrganizationPublicSharing()
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $tableName = 'dates';
        $tableId = $this->_client->createTable(
            $bucketId,
            $tableName,
            new CsvFile(__DIR__ . '/../../_data/dates.csv')
        );

        $response = $this->_client->shareBucket($bucketId, [
            'sharing' => 'organization-project',
        ]);

        $this->assertArrayHasKey('displayName', $response);

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Test Token')
            ->setCanManageBuckets(true)
            ->setExpiresIn(3600)
        ;

        $tokenId = $this->_client2->createToken($tokenOptions);
        $token = $this->_client2->getToken($tokenId);

        $client = new Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $response = $client->verifyToken();
        $this->assertArrayHasKey('owner', $response);
        $this->assertArrayHasKey('id', $response['owner']);
        $this->assertArrayHasKey('name', $response['owner']);
        $linkedBucketProject = $response['owner'];

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
                $displayName
            );
            $this->fail('bucket can\'t be linked with same displayName');
        } catch (ClientException $e) {
            $this->assertEquals("The display name \"".$displayName."\" already exists in project.", $e->getMessage());
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
        }

        try {
            $linkedBucketId = $client->linkBucket(
                'organization-project-test'. time(),
                self::STAGE_IN,
                $sharedBuckets[0]['project']['id'],
                $sharedBuckets[0]['id'],
                '&&&&&&'
            );
            $this->fail('bucket can\'t be linked with same displayName');
        } catch (ClientException $e) {
            $this->assertEquals(
                'Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed in table name.',
                $e->getMessage()
            );
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }

        $this->assertArrayHasKey('tables', $linkedBucket['sourceBucket']);
        $this->assertSame($tableId, $linkedBucket['sourceBucket']['tables'][0]['id']);
        $this->assertSame($tableName, $linkedBucket['sourceBucket']['tables'][0]['name']);

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
        $this->assertArrayHasKey("linkedBy", $sharedBucket);
        $this->assertCount(2, $sharedBucket['linkedBy']);

        // verify that the listed linked buckets contains the linked bucket
        $linkedBucketKey = array_search('in.c-organization-project-test', array_column($sharedBucket['linkedBy'], 'id'));
        $this->assertNotFalse($linkedBucketKey);
        $listedLinkedBucket = $sharedBucket['linkedBy'][$linkedBucketKey];
        $this->assertArrayHasKey("project", $listedLinkedBucket);
        $this->assertEquals($linkedBucketProject['id'], $listedLinkedBucket['project']['id']);
        $this->assertEquals($linkedBucketProject['name'], $listedLinkedBucket['project']['name']);
        $this->assertArrayHasKey("created", $listedLinkedBucket);
        $this->assertEquals($linkedBucket['created'], $listedLinkedBucket['created']);

        // verify the listed linked buckets includes the self-linked bucket
        $selfLinkedBucketKey = array_search('in.c-same-project-link-test', array_column($sharedBucket['linkedBy'], 'id'));
        $this->assertNotFalse($selfLinkedBucketKey);
        $listedSelfLinkedBucket = $sharedBucket['linkedBy'][$selfLinkedBucketKey];
        $this->assertArrayHasKey("project", $listedSelfLinkedBucket);
        $this->assertEquals($sharedBuckets[0]['project']['id'], $listedSelfLinkedBucket['project']['id']);
        $this->assertEquals($sharedBuckets[0]['project']['name'], $listedSelfLinkedBucket['project']['name']);
        $this->assertArrayHasKey("created", $listedSelfLinkedBucket);
        $this->assertEquals($selfLinkedBucket['created'], $listedSelfLinkedBucket['created']);

        // buckets list should include linked buckets
        $buckets = $this->_client->listBuckets(['include' => 'linkedBuckets']);
        $listedSharedBucket = (array) array_values(array_filter($buckets, function ($listBucket) use ($bucketId) {
            return ($listBucket['id'] === $bucketId);
        }))[0];

        $this->assertArrayHasKey("linkedBy", $listedSharedBucket);
        $this->assertCount(2, $listedSharedBucket['linkedBy']);

        $sameListedSharedBucket = (array) array_values(array_filter($buckets, function ($listBucket) use ($selfLinkedBucketId) {
            return ($listBucket['id'] === $selfLinkedBucketId);
        }))[0];

        $this->assertArrayHasKey('tables', $sameListedSharedBucket['sourceBucket']);
        $this->assertSame($tableId, $sameListedSharedBucket['sourceBucket']['tables'][0]['id']);
        $this->assertSame($tableName, $sameListedSharedBucket['sourceBucket']['tables'][0]['name']);

        // user should be also able to delete the linked bucket
        $client->dropBucket($linkedBucketId);
    }

    public function testNonOrganizationAdminInToken()
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);
        $linkedId = $this->_client->linkBucket(
            "linked-" . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // new token creation
        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Test Token')
            ->setCanManageBuckets(true)
            ->setExpiresIn(3600)
        ;

        $tokenId = $this->_client->createToken($tokenOptions);
        $token = $this->_client->getToken($tokenId);

        $client = new Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        $client->verifyToken();

        $this->assertEmpty($client->listSharedBuckets());

        try {
            $client->shareBucket($bucketId);
            $this->fail('`shareBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->unshareBucket($bucketId);
            $this->fail('`unshareBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        try {
            $client->linkBucket(
                "linked-" . time(),
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
            $client->dropBucket($linkedId);
            $this->fail('`dropBucket` should fail with `accessDenied` error');
        } catch (ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucket($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $this->_client->unshareBucket($bucketId);
        $this->assertFalse($this->_client->isSharedBucket($bucketId));

        // sharing twice
        $this->_client->shareBucket($bucketId);

        try {
            $this->_client->shareBucket($bucketId);
            $this->fail("sharing twice should fail");
        } catch (ClientException $e) {
            $this->assertEquals(400, $e->getCode());
            $this->assertEquals('storage.buckets.shareTwice', $e->getStringCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketChangeType($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        // first share
        $targetProjectId = $this->clientAdmin2InSameOrg->verifyToken()['owner']['id'];
        $this->_client->shareBucketToProjects($bucketId, [$targetProjectId]);

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('specific-projects', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertNotEmpty($sharedBucket['sharingParameters']);
        $this->assertInternalType('array', $sharedBucket['sharingParameters']);

        // first reshare
        $this->_client->changeBucketSharing($bucketId, 'organization-project');

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization-project', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertInternalType('array', $sharedBucket['sharingParameters']);

        // second reshare
        $this->_client->changeBucketSharing($bucketId, 'organization');

        $sharedBucket = $this->_client->getBucket($bucketId);
        $this->assertArrayHasKey('sharing', $sharedBucket);
        $this->assertEquals('organization', $sharedBucket['sharing']);
        $this->assertArrayHasKey('sharingParameters', $sharedBucket);
        $this->assertEmpty($sharedBucket['sharingParameters']);
        $this->assertInternalType('array', $sharedBucket['sharingParameters']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testShareBucketChangeTypeOnUnsharedBucket($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        try {
            $this->_client->changeBucketSharing($bucketId, 'organization-project');
            $this->fail('change of sharing type of non-shared bucket should\'nt be possible');
        } catch (ClientException $e) {
            $this->assertEquals('The bucket out.c-API-sharing is not shared.', $e->getMessage());
            $this->assertEquals('storage.bucket.notShared', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testSharedBuckets($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $tableName = 'transactions';
        $tableId = $this->_client->createTable(
            $bucketId,
            $tableName,
            new CsvFile(__DIR__ . '/../../_data/transactions.csv')
        );

        // ensure that sharing data is not output for unshared bucket
        $this->assertFalse($this->_client->isSharedBucket($bucketId));
        $bucketBeforeSharing = $this->_client->getBucket($bucketId);
        $this->assertArrayNotHasKey('sharedBy', $bucketBeforeSharing);
        $this->assertArrayNotHasKey('sharingParameters', $bucketBeforeSharing);

        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $verifyTokenResponse = $this->_client->verifyToken();
        $this->assertArrayHasKey('owner', $verifyTokenResponse);

        $this->assertArrayHasKey('id', $verifyTokenResponse['owner']);
        $this->assertArrayHasKey('name', $verifyTokenResponse['owner']);

        $project = $verifyTokenResponse['owner'];

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        foreach ($response as $sharedBucket) {
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

            $this->assertCount(1, $sharedBucket['tables']);

            $sharedBucketTable = reset($sharedBucket['tables']);

            $this->assertArrayHasKey('id', $sharedBucketTable);
            $this->assertArrayHasKey('name', $sharedBucketTable);

            $this->assertEquals($tableId, $sharedBucketTable['id']);
            $this->assertEquals($tableName, $sharedBucketTable['name']);
        }

        $this->sharedBucketDetailTest(
            $bucketId,
            $tableId,
            $project
        );

        $this->sharedBucketDetailWithAliasTableMetadataTest(
            $bucketId,
            $tableId,
            $project
        );
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testSharedBucketsWithInclude($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);

        $tableName = 'numbers';
        $tableId = $this->_client->createTable(
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

        $this->_client->shareBucket($bucketId);
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
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkBucketDry($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);
        $sourceBucket = $this->_client->getBucket($bucketId);

        $this->_client->shareBucket($bucketId);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket("linked-" . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);

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
        $this->assertArrayHasKey("linkedBy", $sharedBucket);
        $this->assertCount(1, $sharedBucket['linkedBy']);
        $this->assertArrayHasKey("project", $sharedBucket['linkedBy'][0]);
        $this->assertArrayHasKey("created", $sharedBucket['linkedBy'][0]);
        $this->assertEquals($bucket['created'], $sharedBucket['linkedBy'][0]['created']);
        $this->assertArrayHasKey("id", $sharedBucket['linkedBy'][0]);
        $this->assertEquals($id, $sharedBucket['linkedBy'][0]['id']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkBucketToOrganizationDeletePermissions($backend)
    {
        $this->initTestBuckets($backend);
        $bucketId = reset($this->_bucketIds);
        $this->_client->shareBucket($bucketId);

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket("linked-" . time(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Test Token')
            ->setExpiresIn(3600)
            ->setCanManageBuckets(true)
        ;

        $tokenId = $this->_client2->createToken($tokenOptions);
        $token = $this->_client2->getToken($tokenId);

        $client = new Client([
            'token' => $token['token'],
            'url' => STORAGE_API_URL,
        ]);

        try {
            $client->dropBucket($linkedBucketId);
            $this->fail('non-organization member should not be able to delete bucket');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
            $this->assertEquals(403, $e->getCode());
        }

        // organization member should be able to delete linked bucket
        $this->_client2->dropBucket($linkedBucketId);
    }

    public function testBucketCannotBeLinkedMoreTimes()
    {
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);

        $response = $this->_client2->listSharedBuckets();
        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket("linked-" . uniqid(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);
        try {
            $this->_client2->linkBucket("linked-" . uniqid(), 'out', $sharedBucket['project']['id'], $sharedBucket['id']);
            $this->fail('bucket should not be linked');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
        }
    }

    private function validateTablesMetadata($sharedBucketId, $linkedBucketId)
    {
        $fieldNames = [
            'name',
            'columns',
            'primaryKey',
            'name',
            'dataSizeBytes',
            'rowsCount',
            'lastImportDate',
        ];

        $tables = $this->_client->listTables($sharedBucketId, ['include' => 'columns']);
        $linkedTables = $this->_client2->listTables($linkedBucketId, ['include' => 'columns']);

        foreach ($tables as $i => $table) {
            foreach ($fieldNames as $fieldName) {
                $this->assertEquals(
                    $table[$fieldName],
                    $linkedTables[$i][$fieldName],
                    sprintf("Bad value for `%s` metadata attribute", $fieldName)
                );
            }

            $data = $this->_client->getTableDataPreview($table['id']);
            $linkedData = $this->_client2->getTableDataPreview($linkedTables[$i]['id']);

            $this->assertLinesEqualsSorted($data, $linkedData);
        }
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testLinkedBucket($backend)
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

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(self::STAGE_OUT),
            'languages-out',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $metadataApi = new Metadata($this->_client);
        $testMetadata = [
            [
                "key" => "test_metadata_key1",
                "value" => "testval",
            ],
            [
                "key" => "test_metadata_key2",
                "value" => "testval",
            ],
        ];

        $columnId = $table2Id . '.id';
        $expectedMetadata = $metadataApi->postColumnMetadata($columnId, self::TEST_METADATA_PROVIDER, $testMetadata);

        $aliasTableId = $this->_client->createAliasTable(
            $bucketId,
            $table2Id,
            'languages-alias'
        );

        $this->_client->shareBucket($bucketId);

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

        $this->validateTablesMetadata($bucketId, $linkedBucketId);


        // new import
        $this->_client->writeTable(
            $tableId,
            new CsvFile(__DIR__ . '/../../_data/pk.simple.increment.csv'),
            [
                'primaryKey' => 'id',
                'incremental' => true,
            ]
        );

        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // remove primary key
        $this->_client->removeTablePrimaryKey($tableId);
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // add primary key
        $this->_client->createTablePrimaryKey($tableId, ['id', 'name']);
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // add column
        $this->_client->addTableColumn($tableId, 'fake');
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

        // delete rows
        $this->_client->deleteTableRows($tableId, [
            'whereColumn' => 'id',
            'whereValues' => ['new'],
        ]);
        $this->validateTablesMetadata($bucketId, $linkedBucketId);

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
        $this->validateTablesMetadata($bucketId, $linkedBucketId);
        $aliasTable = $this->_client->getTable($aliasId, ['include' => 'columnMetadata']);
        $this->assertSame($expectedMetadata, $aliasTable['sourceTable']['columnMetadata']['id']);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testRestrictedDrop($backend)
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

        $this->_client->shareBucket($bucketId);

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
            $this->_client->dropBucket($bucketId);
            $this->fail('Shared bucket delete should fail');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.alreadyLinked', $e->getStringCode());
        }

        $this->validateTablesMetadata($bucketId, $linkedBucketId);
    }

    /**
     * @dataProvider sharingBackendData
     * @throws ClientException
     */
    public function testForcedDrop($backend)
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

        $this->_client->shareBucket($bucketId);

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

        $tables = $this->_client->listTables($bucketId);
        $this->assertCount(1, $tables);

        foreach ($this->_client->listTables($bucketId) as $table) {
            // column drop
            $this->_client->deleteTableColumn($table['id'], 'name', ['force' =>  true]);

            $detail = $this->_client->getTable($table['id']);
            $this->assertEquals(['id'], $detail['columns']);

            $this->validateTablesMetadata($bucketId, $linkedBucketId);

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
    public function testWorkspaceLoadData($sharingBackend, $workspaceBackend)
    {
        //setup test tables
        $this->deleteAllWorkspaces();
        $this->initTestBuckets($sharingBackend);
        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $secondBucketId = $this->getTestBucketId(self::STAGE_OUT);

        $table1Id = $this->_client->createTable(
            $bucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $table2Id = $this->_client->createTable(
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
        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);
        $sharedBucket = reset($response);

        $linkedId = $this->_client2->linkBucket(
            "linked-" . time(),
            'out',
            $sharedBucket['project']['id'],
            $sharedBucket['id']
        );

        // share and unshare second bucket - test that it doesn't break permissions of first linked bucket
        $this->_client->shareBucket($secondBucketId);
        $sharedBucket2 = array_values(array_filter($this->_client->listSharedBuckets(), function ($bucket) use ($secondBucketId) {
            return $bucket['id'] === $secondBucketId;
        }))[0];
        $linked2Id = $this->_client2->linkBucket(
            "linked-2-" . time(),
            'out',
            $sharedBucket2['project']['id'],
            $sharedBucket2['id']
        );
        $this->_client2->dropBucket($linked2Id);


        $mapping1 = array(
            "source" => str_replace($bucketId, $linkedId, $table1Id),
            "destination" => "languagesLoaded"
        );

        $mapping2 = array(
            "source" => str_replace($bucketId, $linkedId, $table2Id),
            "destination" => "numbersLoaded"
        );

        $mapping3 = array(
            "source" => str_replace($bucketId, $linkedId, $table3Id),
            "destination" => "numbersAliasLoaded"
        );

        // init workspace
        $workspaces = new Workspaces($this->_client2);
        $workspace = $workspaces->createWorkspace([
            "backend" => $workspaceBackend
        ]);

        $input = array($mapping1, $mapping2, $mapping3);

        // test if job is created and listed
        $initialJobs = $this->_client2->listJobs();
        $runId = $this->_client2->generateRunId();
        $this->_client2->setRunId($runId);
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => $input));
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
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersAliasLoaded"), $tables);

        // check table structure and data
        $data = $backend->fetchAll("languagesLoaded", \PDO::FETCH_ASSOC);
        $this->assertCount(2, $data[0], 'there should be two columns');
        $this->assertArrayHasKey('id', $data[0]);
        $this->assertArrayHasKey('name', $data[0]);
        $this->assertArrayEqualsSorted(
            Client::parseCsv(file_get_contents(__DIR__ . '/../../_data/languages.csv'), true, ",", '"'),
            $data,
            'id'
        );

        // now we'll load another table and use the preserve parameters to check that all tables are present
        // lets create it now to see if the table permissions are correctly propagated
        $table3Id = $this->_client->createTable(
            $bucketId,
            'numbersLater',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );

        $mapping3 = array("source" => str_replace($bucketId, $linkedId, $table3Id), "destination" => "table3");
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3), "preserve" => true));

        $tables = $backend->getTables();

        $this->assertCount(4, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);
        $this->assertContains($backend->toIdentifier("languagesLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersLoaded"), $tables);
        $this->assertContains($backend->toIdentifier("numbersAliasLoaded"), $tables);

        // now we'll try the same load, but it should clear the workspace first (preserve is false by default)
        $workspaces->loadWorkspaceData($workspace['id'], array("input" => array($mapping3)));

        $tables = $backend->getTables();
        $this->assertCount(1, $tables);
        $this->assertContains($backend->toIdentifier("table3"), $tables);



        // unload validation
        $connection = $workspace['connection'];

        $backend = null; // force disconnect of same SNFLK connection
        $db = $this->getDbConnection($connection);

        $db->query("create table \"test.Languages3\" (
			\"Id\" integer not null,
			\"Name\" varchar not null
		);");
        $db->query("insert into \"test.Languages3\" (\"Id\", \"Name\") values (1, 'cz'), (2, 'en');");

        try {
            $this->_client2->createTableAsyncDirect($linkedId, array(
                'name' => 'languages3',
                'dataWorkspaceId' => $workspace['id'],
                'dataTableName' => 'test.Languages3',
            ));

            $this->fail('Unload to liked bucket should fail with access exception');
        } catch (ClientException $e) {
            $this->assertEquals('accessDenied', $e->getStringCode());
        }
    }

    public function testCloneLinkedBucket()
    {
        $this->deleteAllWorkspaces();
        $this->initTestBuckets(self::BACKEND_SNOWFLAKE);

        // prepare source data
        $sourceBucketId = $this->getTestBucketId();
        $table1Id = $this->_client->createTable(
            $this->getTestBucketId(),
            'languagesDetails',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $this->_client->shareBucket($sourceBucketId);

        $table2Id = $this->_client->createTable(
            $this->getTestBucketId(),
            'numbers',
            new CsvFile(__DIR__ . '/../../_data/numbers.csv')
        );


        $table3Id = $this->_client->createTable(
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
            "linked-" . uniqid(),
            'out',
            $sourceProjectId,
            $sourceBucketId
        );

        // load data into workspace in destination project
        $workspacesClient = new Workspaces($this->_client2);
        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

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
                ]
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
                ]
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
                ]
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

    private function sharedBucketDetailTest($bucketId, $tableId, $project)
    {
        $columnId = $tableId . '.transid';
        $this->prepareTestMetadata($bucketId, $tableId, $columnId);

        $newBucketId = $this->_bucketIds['in'];
        $this->_client->shareBucket($newBucketId);

        $this->assertSame(2, count($this->_client2->listSharedBuckets()));

        $response = $this->_client2->getSharedBucketDetail(
            $project['id'],
            $newBucketId
        );

        $this->assertSame($newBucketId, $response['id']);

        $response = $this->_client2->getSharedBucketDetail(
            $project['id'],
            $bucketId
        );

        $this->assertArrayHasKey('id', $response);
        $this->assertArrayHasKey('description', $response);
        $this->assertArrayHasKey('project', $response);
        $this->assertArrayHasKey('tables', $response);
        $this->assertArrayHasKey('created', $response);
        $this->assertArrayHasKey('lastChangeDate', $response);
        $this->assertArrayHasKey('dataSizeBytes', $response);
        $this->assertArrayHasKey('rowsCount', $response);
        $this->assertArrayHasKey('backend', $response);

        $this->assertArrayHasKey('id', $response['project']);
        $this->assertArrayHasKey('name', $response['project']);

        $this->assertSame($bucketId, $response['id']);
        $this->assertEquals($response['project']['id'], $project['id']);
        $this->assertEquals($response['project']['name'], $project['name']);

        $this->assertArrayHasKey('tables', $response);

        foreach ($response['tables'] as $table) {
            $this->assertSame($tableId, $table['id']);
            $this->assertSame('transactions', $table['name']);
            $this->assertArrayNotHasKey('metadata', $table);
            $this->assertArrayNotHasKey('columnMetadata', $table);
            $this->assertArrayNotHasKey('columns', $table);
        }

        $response = $this->_client2->getSharedBucketDetail(
            $project['id'],
            $bucketId,
            [
                'include' => 'metadata,columns,tableMetadata,columnMetadata'
            ]
        );

        $this->assertCount(1, $response['metadata']);

        $sharedBucketMetadata = $response['metadata'][0];

        $this->assertArrayHasKey('id', $sharedBucketMetadata);
        $this->assertArrayHasKey('key', $sharedBucketMetadata);
        $this->assertArrayHasKey('value', $sharedBucketMetadata);
        $this->assertArrayHasKey('provider', $sharedBucketMetadata);
        $this->assertArrayHasKey('timestamp', $sharedBucketMetadata);

        $this->assertEquals('test', $sharedBucketMetadata['provider']);
        $this->assertEquals('test.metadata.key', $sharedBucketMetadata['key']);
        $this->assertEquals('test.metadata.value', $sharedBucketMetadata['value']);

        foreach ($response['tables'] as $table) {
            $this->assertArrayHasKey('metadata', $table);
            $this->assertArrayHasKey('columns', $table);
            $this->assertArrayHasKey('columnMetadata', $table);

            $this->assertCount(1, $table['metadata']);
            $tableMetadata = $table['metadata'][0];

            $this->assertArrayHasKey('id', $tableMetadata);
            $this->assertArrayHasKey('key', $tableMetadata);
            $this->assertArrayHasKey('value', $tableMetadata);
            $this->assertArrayHasKey('provider', $tableMetadata);
            $this->assertArrayHasKey('timestamp', $tableMetadata);

            $this->assertEquals('test', $tableMetadata['provider']);
            $this->assertEquals('test.metadata.key', $tableMetadata['key']);
            $this->assertEquals('test.metadata.value', $tableMetadata['value']);

            $columns = $table['columns'];

            $tableColumns = $this->_client->getTable($tableId)['columns'];
            $this->assertSame($tableColumns, $columns);

            $this->assertCount(1, $table['columnMetadata']['transid']);
            $columnsMetadata = $table['columnMetadata']['transid'][0];

            $this->assertArrayHasKey('id', $columnsMetadata);
            $this->assertArrayHasKey('key', $columnsMetadata);
            $this->assertArrayHasKey('value', $columnsMetadata);
            $this->assertArrayHasKey('provider', $columnsMetadata);
            $this->assertArrayHasKey('timestamp', $columnsMetadata);

            $this->assertEquals('test', $columnsMetadata['provider']);
            $this->assertEquals('test.metadata.key', $columnsMetadata['key']);
            $this->assertEquals('test.metadata.value', $columnsMetadata['value']);
        }

        try {
            $this->_client2->getSharedBucketDetail(
                '',
                $bucketId
            );

            $this->fail('Wrong Project Id');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.buckets.wrongProjectId', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        try {
            $this->_client2->getSharedBucketDetail(
                $project['id'],
                ''
            );

            $this->fail('Wrong Bucket Id');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.buckets.wrongBucketId', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        try {
            $this->_client2->getSharedBucketDetail(
                $this->clientAdmin3InOtherOrg->verifyToken()['owner']['id'],
                $bucketId
            );
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.buckets.projectNotFound', $e->getStringCode());
            $this->assertEquals(404, $e->getCode());
        }
    }

    private function sharedBucketDetailWithAliasTableMetadataTest($bucketId, $tableId, $project)
    {
        $columnId = $tableId . '.transid';
        $mdList = $this->prepareTestMetadata($bucketId, $tableId, $columnId);

        // create alias of alias
        $this->_client->createAliasTable(
            $bucketId,
            $tableId,
            'tableAlias'
        );
        $tableAliasAliasName = 'tableAliasAlias';
        $this->_client->createAliasTable(
            $bucketId,
            $bucketId . '.tableAlias',
            $tableAliasAliasName
        );

        $response = $this->_client2->getSharedBucketDetail(
            $project['id'],
            $bucketId,
            [
                'include' => 'columns,tableMetadata,columnMetadata'
            ]
        );

        $aliasAliasTableId = $bucketId . '.' . $tableAliasAliasName;
        $tables = array_values(array_filter($response['tables'], function ($table) use ($aliasAliasTableId) {
            return $table['id'] === $aliasAliasTableId;
        }));

        // the metadata should be propagated from the source table
        $this->assertNotEmpty($tables[0]['sourceTable']['columnMetadata']['transid']);
        $this->assertEquals(
            $mdList,
            $tables[0]['sourceTable']['columnMetadata']['transid']
        );

        $this->assertArrayHasKey('metadata', $tables[0]);
        $this->assertArrayHasKey('columns', $tables[0]);
        $this->assertArrayHasKey('columnMetadata', $tables[0]);

        $this->assertSame(0, count($tables[0]['metadata']));

        $columns = $tables[0]['columns'];
        $tableColumns = $this->_client->getTable($tableId)['columns'];
        $this->assertSame($tableColumns, $columns);

        $this->assertSame(0, count($tables[0]['columnMetadata']));

        $columnId = $aliasAliasTableId . '.id';

        $mdList = $this->prepareTestMetadata($bucketId, $aliasAliasTableId, $columnId);

        $response = $this->_client2->getSharedBucketDetail(
            $project['id'],
            $bucketId,
            [
                'include' => 'columns,tableMetadata,columnMetadata'
            ]
        );

        $this->assertSame($bucketId, $response['id']);

        $tables = array_values(array_filter($response['tables'], function ($table) use ($aliasAliasTableId) {
            return $table['id'] === $aliasAliasTableId;
        }));

        $this->assertNotEmpty($tables[0]['columnMetadata']['id']);
        $this->assertEquals(
            $mdList,
            $tables[0]['columnMetadata']['id']
        );

        $this->assertArrayHasKey('metadata', $tables[0]);
        $this->assertArrayHasKey('columns', $tables[0]);
        
        $this->assertSame(5, count($tables[0]['columns']));
        foreach ($tables[0]['columns'] as $column) {
            $this->assertTrue(in_array($column, $tableColumns));
        }

        $this->assertCount(1, $tables[0]['metadata']);
        $tableMetadata = $tables[0]['metadata'][0];

        $this->assertArrayHasKey('id', $tableMetadata);
        $this->assertArrayHasKey('key', $tableMetadata);
        $this->assertArrayHasKey('value', $tableMetadata);
        $this->assertArrayHasKey('provider', $tableMetadata);
        $this->assertArrayHasKey('timestamp', $tableMetadata);

        $this->assertEquals('test', $tableMetadata['provider']);
        $this->assertEquals('test.metadata.key', $tableMetadata['key']);
        $this->assertEquals('test.metadata.value', $tableMetadata['value']);
    }

    private function prepareTestMetadata($bucketId, $tableId, $columnId)
    {
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

        $metadataClient->postTableMetadata(
            $tableId,
            'test',
            [
                [
                    'key' => 'test.metadata.key',
                    'value' => 'test.metadata.value',
                ],
            ]
        );

        $metadataClient->postColumnMetadata(
            $columnId,
            'test',
            [
                [
                    'key' => 'test.metadata.key',
                    'value' => 'test.metadata.value',
                ],
            ]
        );

        return $metadataClient->listColumnMetadata($columnId);
    }

    public function testGetSharedBucketDetailForNonExistBucket()
    {
        $response = $this->_client->verifyToken();
        $this->assertArrayHasKey('owner', $response);

        $this->assertArrayHasKey('id', $response['owner']);
        $this->assertArrayHasKey('name', $response['owner']);

        $project = $response['owner'];

        try {
            $this->_client2->getSharedBucketDetail(
                $project['id'],
                'non-exist',
                [
                    'include' => 'columns'
                ]
            );
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.notFound', $e->getStringCode());
            $this->assertEquals(404, $e->getCode());
        }
    }

    public function invalidSharingTypeData()
    {
        return [
            'non existing type' => [
                'global',
            ],
            'sharing to specifix projects' => [
                'specific-projects',
            ],
            'sharing to specifix users' => [
                'specific-users',
            ],
        ];
    }
}
