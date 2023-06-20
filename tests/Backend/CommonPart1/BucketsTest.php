<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;

class BucketsTest extends StorageApiTestCase
{
    use EventTesterUtils;

    /** @var BranchAwareClient|Client */
    private $_testClient;

    private ClientProvider $clientProvider;

    public function setUp(): void
    {
        parent::setUp();
        $this->clientProvider = new ClientProvider($this);
        [$devBranchType, $userRole] = $this->getProvidedData();
        [$this->_client, $this->_testClient] = $this->setUpForProtectedDevBranch($this->clientProvider, $devBranchType, $userRole);

        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            // buckets must be created in branch that the tests run in
            $this->initEmptyTestBucketsForParallelTests([self::STAGE_OUT, self::STAGE_IN], $this->_testClient);
        } elseif ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
            $this->initEmptyTestBucketsForParallelTests();
        } else {
            throw new \Exception(sprintf('Unknown devBranchType "%s"', $devBranchType));
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketsList(string $devBranchType, string $userRole): void
    {
        $buckets = $this->_testClient->listBuckets();

        $this->assertTrue(count($buckets) >= 2);

        $inBucketFound = false;
        $outBucketFound = false;
        foreach ($buckets as $bucket) {
            if ($bucket['id'] == $this->getTestBucketId(self::STAGE_IN)) {
                $inBucketFound = true;
            }
            if ($bucket['id'] == $this->getTestBucketId(self::STAGE_OUT)) {
                $outBucketFound = true;
            }
        }
        $this->assertTrue($inBucketFound);
        $this->assertTrue($outBucketFound);

        $firstBucket = reset($buckets);
        $this->assertArrayHasKey('displayName', $firstBucket);
        $this->assertNotEquals('', $firstBucket['displayName']);
        $this->assertArrayHasKey('created', $firstBucket);
        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            $this->assertEquals($this->clientProvider->getExistingBranchForTestCase()['id'], $firstBucket['idBranch']);
        } elseif ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
            // PM can't read branches in prod, so needs to be privileged token
            $branchesApi = new DevBranches($this->_client);
            $defaultBranch = $branchesApi->getDefaultBranch();
            $this->assertEquals($defaultBranch['id'], $firstBucket['idBranch']);
        } else {
            throw new \Exception(sprintf('Unknown devBranchType "%s"', $devBranchType));
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketDetail(string $devBranchType, string $userRole): void
    {
        $displayName = 'Romanov-Bucket';
        $bucketName = 'BucketsTest_testBucketDetail';

        if ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
            $branch = (new DevBranches($this->_client))->getDefaultBranch();
        } else {
            $branch = $this->clientProvider->getExistingBranchForTestCase();
        }

        $tokenData = $this->_testClient->verifyToken();
        $this->dropBucketIfExists($this->_testClient, 'in.c-' . $bucketName, true);
        $bucketId = $this->_testClient->createBucket($bucketName, self::STAGE_IN);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEquals($branch['id'], $bucket['idBranch']);

        $this->assertEquals($tokenData['owner']['defaultBackend'], $bucket['backend']);
        $this->assertNotEquals($displayName, $bucket['displayName']);

        $asyncBucketDisplayName = $displayName . '-async';
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $asyncBucketDisplayName, true);
        $this->_testClient->updateBucket($bucketUpdateOptions);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEquals($asyncBucketDisplayName, $bucket['displayName']);

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucket = $this->_testClient->updateBucket($bucketUpdateOptions);
        try {
            $this->_testClient->createBucket($displayName, self::STAGE_IN);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('The display name "Romanov-Bucket" already exists in project.', $e->getMessage());
        }

        $this->assertEquals($displayName, $bucket['displayName']);

        $bucketUpdateOptions = new BucketUpdateOptions($this->getTestBucketId(), $displayName);
        try {
            $this->_testClient->updateBucket($bucketUpdateOptions);
            $this->fail('The display name already exists in project');
        } catch (ClientException $e) {
            $this->assertEquals('The display name "' . $displayName . '" already exists in project.', $e->getMessage());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, '$$$$$');
        try {
            $this->_testClient->updateBucket($bucketUpdateOptions);
            $this->fail('Wrong display name');
        } catch (ClientException $e) {
            $this->assertEquals('Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEquals($displayName, $bucket['displayName']);

        // renaming bucket to the same name should be successful
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucket = $this->_testClient->updateBucket($bucketUpdateOptions);

        $this->_testClient->dropBucket($bucket['id'], ['async' => true]);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketEvents(string $devBranchType, string $userRole): void
    {
        $this->initEvents($this->_testClient);

        // create bucket event
        $this->_testClient->listTables($this->getTestBucketId());

        $assertCallback = function ($events) use ($devBranchType) {
            $this->assertCount(1, $events);
            // check bucket events
            $this->assertSame('storage.tablesListed', $events[0]['event']);
            $this->assertSame('Listed tables', $events[0]['message']);
            $this->assertSame($this->getTestBucketId(), $events[0]['objectId']);
            $this->assertSame('bucket', $events[0]['objectType']);
            if ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
                $this->assertSame($this->getDefaultBranchId($this), $events[0]['idBranch']);
            } else {
                $this->assertSame($this->_testClient->getCurrentBranchId(), $events[0]['idBranch']);
            }
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tablesListed')
            ->setTokenId($this->tokenId)
            ->setObjectId($this->getTestBucketId());
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketsListWithEmptyIncludeParameter(string $devBranchType, string $userRole): void
    {
        $buckets = $this->_testClient->listBuckets([
            'include' => '',
        ]);

        $firstBucket = reset($buckets);
        $this->assertArrayNotHasKey('metadata', $firstBucket);
        $this->assertArrayNotHasKey('linkedBuckets', $firstBucket);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketsListWithIncludeMetadata(string $devBranchType, string $userRole): void
    {
        $buckets = $this->_testClient->listBuckets([
            'include' => 'metadata',
        ]);
        $firstBucket = array_filter($buckets, function ($bucket) {
            return $bucket['id'] === $this->_bucketIds[self::STAGE_IN];
        });

        $firstBucket = reset($firstBucket);

        self::assertArrayHasKey('metadata', $firstBucket);
        self::assertEmpty($firstBucket['metadata']);

        $metadataApi = new Metadata($this->_testClient);
        $metadataApi->postBucketMetadata($firstBucket['id'], 'storage-php-client-test', [
            [
                'key' => 'test-key',
                'value' => 'test-value',
            ],
        ]);

        $buckets = $this->_testClient->listBuckets([
            'include' => 'metadata',
        ]);

        $filteredBuckets = array_filter($buckets, function ($bucket) {
            return $bucket['id'] === $this->_bucketIds[self::STAGE_IN];
        });

        $firstBucket = reset($filteredBuckets);
        self::assertArrayHasKey('metadata', $firstBucket);
        self::assertCount(1, $firstBucket['metadata']);
        self::assertArrayHasKey('key', $firstBucket['metadata'][0]);
        self::assertEquals('test-key', $firstBucket['metadata'][0]['key']);
        self::assertArrayHasKey('value', $firstBucket['metadata'][0]);
        self::assertEquals('test-value', $firstBucket['metadata'][0]['value']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketCreateWithInvalidBackend(string $devBranchType, string $userRole): void
    {
        try {
            $this->_testClient->createBucket('unknown-backend', 'in', 'desc', 'redshit');
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketManipulation(string $devBranchType, string $userRole): void
    {
        $tokenData = $this->_testClient->verifyToken();

        $bucketData = [
            'name' => 'test',
            'displayName' => 'test-display-name',
            'stage' => 'in',
            'description' => 'this is just a test',
        ];

        $testBucketId = $bucketData['stage'] . '.c-' . $bucketData['name'];

        $this->dropBucketIfExists($this->_testClient, $testBucketId, true);

        $newBucketId = $this->_testClient->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName'],
        );

        $importFile = __DIR__ . '/../../_data/languages.csv';
        if ($devBranchType === ClientProvider::DEV_BRANCH) {
            self::markTestIncomplete('Branched filestorage is not yet ready');
        }
        // create and import data into source table
        $sourceTableId = $this->_testClient->createTableAsync(
            $newBucketId,
            'languages',
            new CsvFile($importFile),
        );

        try {
            $this->_testClient->dropBucket($newBucketId);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }
        try {
            $this->_testClient->dropBucket($newBucketId, ['async' => true]);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }

        $this->_testClient->dropBucket($newBucketId, ['force' => true, 'async' => true]);

        $newBucketId = $this->_testClient->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName'],
        );

        $importFile = __DIR__ . '/../../_data/languages.csv';
        // create and import data into source table
        $sourceTableId = $this->_testClient->createTableAsync(
            $newBucketId,
            'languages',
            new CsvFile($importFile),
        );

        $this->_testClient->dropBucket($newBucketId, ['async' => true, 'force' => true]);

        $newBucketId = $this->_testClient->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName'],
        );

        $newBucket = $this->_testClient->getBucket($newBucketId);
        $this->assertEquals('c-' . $bucketData['name'], $newBucket['name'], 'bucket name');
        $this->assertEquals($bucketData['displayName'], $newBucket['displayName'], 'bucket displayName');
        $this->assertEquals($bucketData['stage'], $newBucket['stage'], 'bucket stage');
        $this->assertEquals($bucketData['description'], $newBucket['description'], 'bucket description');
        $this->assertEquals($tokenData['owner']['defaultBackend'], $newBucket['backend'], 'backend');

        // check if bucket is in list
        $buckets = $this->_testClient->listBuckets();
        $this->assertTrue(in_array($newBucketId, array_map(function ($bucket) {
            return $bucket['id'];
        }, $buckets)));

        try {
            $this->_testClient->createBucket(
                $bucketData['name'] . '-' . time(),
                $bucketData['stage'],
                $bucketData['description'],
                null,
                $bucketData['displayName'],
            );
            $this->fail('Display name already exist for project');
        } catch (ClientException $e) {
            $this->assertEquals('The display name "test-display-name" already exists in project.', $e->getMessage());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
        }

        try {
            $this->_testClient->createBucket(
                $bucketData['name'] . '-' . time(),
                $bucketData['stage'],
                $bucketData['description'],
                null,
                '$$$$$',
            );
            $this->fail('Display name provided is invalid');
        } catch (ClientException $e) {
            $this->assertEquals('Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }

        $this->_testClient->dropBucket($newBucket['id'], ['async' => true]);

        $newBucketId = $this->_testClient->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
        );

        $newBucket = $this->_testClient->getBucket($newBucketId);
        $this->assertEquals('c-' . $bucketData['name'], $newBucket['name'], 'bucket name');
        $this->assertEquals($bucketData['name'], $newBucket['displayName'], 'bucket displayName');
        $this->assertEquals($bucketData['stage'], $newBucket['stage'], 'bucket stage');
        $this->assertEquals($bucketData['description'], $newBucket['description'], 'bucket description');
        $this->assertEquals($tokenData['owner']['defaultBackend'], $newBucket['backend'], 'backend');

        // check if bucket is in list
        $buckets = $this->_testClient->listBuckets();
        $this->assertTrue(in_array($newBucketId, array_map(function ($bucket) {
            return $bucket['id'];
        }, $buckets)));
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketCreateWithoutDescription(string $devBranchType, string $userRole): void
    {
        $this->dropBucketIfExists($this->_testClient, 'in.c-something', true);
        $bucketId = $this->_testClient->createBucket('something', self::STAGE_IN);
        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEmpty($bucket['description']);
        $this->_testClient->dropBucket($bucket['id'], ['async' => true]);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketExists(string $devBranchType, string $userRole): void
    {
        $this->assertTrue($this->_testClient->bucketExists($this->getTestBucketId()));
        $this->assertFalse($this->_testClient->bucketExists('in.ukulele'));
    }

    public function provideComponentsClientTypeBasedOnSuite()
    {
        $this->clientProvider = new ClientProvider($this);
        $this->_client = $this->getDefaultClient();
        $defaultAndBranchProvider = [
            'defaultBranch + production-mananger' => [
                ClientProvider::DEFAULT_BRANCH,
                'production-manager',
            ],
            'devBranch + developer' => [
                ClientProvider::DEV_BRANCH,
                'developer',
            ],
        ];
        $onlyDefaultProvider = [
            'defaultBranch + admin' => [
                ClientProvider::DEFAULT_BRANCH,
                'admin',
            ],
        ];

        if (SUITE_NAME === 'paratest-sox-snowflake') {
            return $defaultAndBranchProvider;
        }

        // it's not set - so it's likely local run
        if (SUITE_NAME === '' || SUITE_NAME === false) {
            // select based on feature
            $token = $this->getDefaultClient()->verifyToken();
            $this->assertArrayHasKey('owner', $token);
            if (in_array('protected-default-branch', $token['owner']['features'], true)) {
                return $defaultAndBranchProvider;
            }
        }

        return $onlyDefaultProvider;
    }

    /**
     * @param ClientProvider $clientProvider
     * @param ClientProvider::*_BRANCH $devBranchType
     * @param string $userRole
     * @return array{0: Client, 1: Client}
     */
    public function setUpForProtectedDevBranch(ClientProvider $clientProvider, string $devBranchType, string $userRole): array
    {
        $hasProjectProtectedDefaultbranch = in_array($userRole, ['reviewer', 'developer', 'production-manager']);

        $client = $clientProvider->getDefaultClient();
        if ($hasProjectProtectedDefaultbranch) {
            // default branch is protected, we need privileged client for production cleanup
            $client = $clientProvider->getDefaultClient(['token' => STORAGE_API_DEFAULT_BRANCH_TOKEN]);
        }

        if ($devBranchType === ClientProvider::DEFAULT_BRANCH && $userRole === 'production-manager') {
            $testClient = $clientProvider->getDefaultClient(['token' => STORAGE_API_DEFAULT_BRANCH_TOKEN]);
        } elseif ($devBranchType === ClientProvider::DEV_BRANCH && $userRole === 'developer') {
            $branchName = $clientProvider->getDevBranchName();
            // dev can create & delete branches in production
            $devBranches = new DevBranches($clientProvider->getDefaultClient(['token' => STORAGE_API_DEVELOPER_TOKEN]));
            $this->deleteBranchesByPrefix($devBranches, $branchName);
            $branch = $devBranches->createBranch($branchName);

            // branched client for dev
            $testClient = $clientProvider->getBranchAwareClient($branch['id'], [
                'token' => STORAGE_API_DEVELOPER_TOKEN,
                'url' => STORAGE_API_URL,
                'backoffMaxTries' => 1,
                'jobPollRetryDelay' => function () {
                    return 1;
                },
            ]);
        } elseif ($devBranchType === ClientProvider::DEFAULT_BRANCH && $userRole === 'admin') {
            // fallback for normal tests
            $testClient = $clientProvider->createClientForCurrentTest();
        } else {
            throw new \Exception(sprintf('Unknown combination of devBranchType "%s" and userRole "%s"', $devBranchType, $userRole));
        }

        return [$client, $testClient];
    }
}
