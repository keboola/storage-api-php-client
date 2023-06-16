<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
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

    private ClientProvider $clientProvider;

    public function setUp(): void
    {
        parent::setUp();
        $this->clientProvider = new ClientProvider($this);
        $this->_client = $this->clientProvider->createClientForCurrentTest();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @param ClientProvider::*_BRANCH $branchType
     */
    public function testBucketsList(string $branchType): void
    {
        $buckets = $this->_client->listBuckets();

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
        if ($branchType === ClientProvider::DEV_BRANCH) {
            $this->assertEquals($this->clientProvider->getExistingBranchForTestCase()['id'], $firstBucket['idBranch']);
        } else {
            $branchesApi = new DevBranches($this->_client);
            $defaultBranch = $branchesApi->getDefaultBranch();
            $this->assertEquals($defaultBranch['id'], $firstBucket['idBranch']);
        }
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @param ClientProvider::*_BRANCH $branchType
     */
    public function testBucketDetail(string $branchType): void
    {
        $displayName = 'Romanov-Bucket';
        $bucketName = 'BucketsTest_testBucketDetail';

        if ($branchType === ClientProvider::DEFAULT_BRANCH) {
            $branch = (new DevBranches($this->_client))->getDefaultBranch();
        } else {
            $branch = $this->clientProvider->getExistingBranchForTestCase();
        }

        $tokenData = $this->_client->verifyToken();
        $this->dropBucketIfExists($this->_client, 'in.c-' . $bucketName, true);
        $bucketId = $this->_client->createBucket($bucketName, self::STAGE_IN);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEquals($branch['id'], $bucket['idBranch']);

        $this->assertEquals($tokenData['owner']['defaultBackend'], $bucket['backend']);
        $this->assertNotEquals($displayName, $bucket['displayName']);

        $asyncBucketDisplayName = $displayName . '-async';
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $asyncBucketDisplayName, true);
        $this->_client->updateBucket($bucketUpdateOptions);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEquals($asyncBucketDisplayName, $bucket['displayName']);

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucket = $this->_client->updateBucket($bucketUpdateOptions);
        try {
            $this->_client->createBucket($displayName, self::STAGE_IN);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('The display name "Romanov-Bucket" already exists in project.', $e->getMessage());
        }

        $this->assertEquals($displayName, $bucket['displayName']);

        $bucketUpdateOptions = new BucketUpdateOptions($this->getTestBucketId(), $displayName);
        try {
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('The display name already exists in project');
        } catch (ClientException $e) {
            $this->assertEquals('The display name "' . $displayName . '" already exists in project.', $e->getMessage());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, '$$$$$');
        try {
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('Wrong display name');
        } catch (ClientException $e) {
            $this->assertEquals('Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEquals($displayName, $bucket['displayName']);

        // renaming bucket to the same name should be successful
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucket = $this->_client->updateBucket($bucketUpdateOptions);

        $this->_client->dropBucket($bucket['id'], ['async' => true]);
    }

    public function testBucketEvents(): void
    {
        $this->initEvents($this->_client);

        // create bucket event
        $this->_client->listTables($this->getTestBucketId());

        $assertCallback = function ($events) {
            $this->assertCount(1, $events);
            // check bucket events
            $this->assertSame('storage.tablesListed', $events[0]['event']);
            $this->assertSame('Listed tables', $events[0]['message']);
            $this->assertSame($this->getTestBucketId(), $events[0]['objectId']);
            $this->assertSame('bucket', $events[0]['objectType']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tablesListed')
            ->setTokenId($this->tokenId)
            ->setObjectId($this->getTestBucketId());
        $this->assertEventWithRetries($this->_client, $assertCallback, $query);
    }

    public function testBucketsListWithEmptyIncludeParameter(): void
    {
        $buckets = $this->_client->listBuckets([
            'include' => '',
        ]);

        $firstBucket = reset($buckets);
        $this->assertArrayNotHasKey('metadata', $firstBucket);
        $this->assertArrayNotHasKey('linkedBuckets', $firstBucket);
    }

    public function testBucketsListWithIncludeMetadata(): void
    {
        $buckets = $this->_client->listBuckets([
            'include' => 'metadata',
        ]);
        $firstBucket = array_filter($buckets, function ($bucket) {
            return $bucket['id'] === $this->_bucketIds[self::STAGE_IN];
        });

        $firstBucket = reset($firstBucket);

        self::assertArrayHasKey('metadata', $firstBucket);
        self::assertEmpty($firstBucket['metadata']);

        $metadataApi = new Metadata($this->_client);
        $metadataApi->postBucketMetadata($firstBucket['id'], 'storage-php-client-test', [
            [
                'key' => 'test-key',
                'value' => 'test-value',
            ],
        ]);

        $buckets = $this->_client->listBuckets([
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

    public function testBucketCreateWithInvalidBackend(): void
    {
        try {
            $this->_client->createBucket('unknown-backend', 'in', 'desc', 'redshit');
            $this->fail('Exception should be thrown');
        } catch (ClientException $e) {
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }
    }

    public function testBucketManipulation(): void
    {
        $tokenData = $this->_client->verifyToken();

        $bucketData = [
            'name' => 'test',
            'displayName' => 'test-display-name',
            'stage' => 'in',
            'description' => 'this is just a test',
        ];

        $testBucketId = $bucketData['stage'] . '.c-' . $bucketData['name'];

        $this->dropBucketIfExists($this->_client, $testBucketId, true);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName'],
        );

        $importFile = __DIR__ . '/../../_data/languages.csv';
        // create and import data into source table
        $sourceTableId = $this->_client->createTableAsync(
            $newBucketId,
            'languages',
            new CsvFile($importFile),
        );

        try {
            $this->_client->dropBucket($newBucketId);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }
        try {
            $this->_client->dropBucket($newBucketId, ['async' => true]);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }

        $this->_client->dropBucket($newBucketId, ['force' => true, 'async' => true]);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName'],
        );

        $importFile = __DIR__ . '/../../_data/languages.csv';
        // create and import data into source table
        $sourceTableId = $this->_client->createTableAsync(
            $newBucketId,
            'languages',
            new CsvFile($importFile),
        );

        $this->_client->dropBucket($newBucketId, ['async' => true, 'force' => true]);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName'],
        );

        $newBucket = $this->_client->getBucket($newBucketId);
        $this->assertEquals('c-' . $bucketData['name'], $newBucket['name'], 'bucket name');
        $this->assertEquals($bucketData['displayName'], $newBucket['displayName'], 'bucket displayName');
        $this->assertEquals($bucketData['stage'], $newBucket['stage'], 'bucket stage');
        $this->assertEquals($bucketData['description'], $newBucket['description'], 'bucket description');
        $this->assertEquals($tokenData['owner']['defaultBackend'], $newBucket['backend'], 'backend');

        // check if bucket is in list
        $buckets = $this->_client->listBuckets();
        $this->assertTrue(in_array($newBucketId, array_map(function ($bucket) {
            return $bucket['id'];
        }, $buckets)));

        try {
            $this->_client->createBucket(
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
            $this->_client->createBucket(
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

        $this->_client->dropBucket($newBucket['id'], ['async' => true]);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
        );

        $newBucket = $this->_client->getBucket($newBucketId);
        $this->assertEquals('c-' . $bucketData['name'], $newBucket['name'], 'bucket name');
        $this->assertEquals($bucketData['name'], $newBucket['displayName'], 'bucket displayName');
        $this->assertEquals($bucketData['stage'], $newBucket['stage'], 'bucket stage');
        $this->assertEquals($bucketData['description'], $newBucket['description'], 'bucket description');
        $this->assertEquals($tokenData['owner']['defaultBackend'], $newBucket['backend'], 'backend');

        // check if bucket is in list
        $buckets = $this->_client->listBuckets();
        $this->assertTrue(in_array($newBucketId, array_map(function ($bucket) {
            return $bucket['id'];
        }, $buckets)));
    }

    public function testBucketCreateWithoutDescription(): void
    {
        $this->dropBucketIfExists($this->_client, 'in.c-something', true);
        $bucketId = $this->_client->createBucket('something', self::STAGE_IN);
        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEmpty($bucket['description']);
        $this->_client->dropBucket($bucket['id'], ['async' => true]);
    }

    public function testBucketExists(): void
    {
        $this->assertTrue($this->_client->bucketExists($this->getTestBucketId()));
        $this->assertFalse($this->_client->bucketExists('in.ukulele'));
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
}
