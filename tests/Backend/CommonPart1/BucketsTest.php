<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\BucketDetailOptions;
use Keboola\StorageApi\Options\BucketOwnerUpdateOptions;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\Test\ClientProvider\ClientProvider;
use Keboola\Test\ClientProvider\TestSetupHelper;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\EventTesterUtils;
use Keboola\Test\Utils\MetadataUtils;

class BucketsTest extends StorageApiTestCase
{
    use EventTesterUtils;
    use MetadataUtils;

    /** @var BranchAwareClient|Client */
    private $_testClient;

    private ClientProvider $clientProvider;

    public function setUp(): void
    {
        parent::setUp();
        $this->clientProvider = new ClientProvider($this);
        [$devBranchType, $userRole] = $this->getProvidedData();
        [$this->_client, $this->_testClient] = (new TestSetupHelper())->setUpForProtectedDevBranch(
            $this->clientProvider,
            $devBranchType,
            $userRole,
        );

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

        $firstBucket = null;
        $inBucketFound = false;
        $outBucketFound = false;
        foreach ($buckets as $bucket) {
            if ($bucket['id'] == $this->getTestBucketId(self::STAGE_IN)) {
                $inBucketFound = true;
                $firstBucket = $bucket;
            }
            if ($bucket['id'] == $this->getTestBucketId(self::STAGE_OUT)) {
                $outBucketFound = true;
            }
        }
        $this->assertTrue($inBucketFound);
        $this->assertTrue($outBucketFound);
        $this->assertNotNull($firstBucket);

        $this->assertArrayHasKey('id', $firstBucket);
        $this->assertSame($this->getTestBucketId(self::STAGE_IN), $firstBucket['id']);
        $this->assertArrayHasKey('name', $firstBucket);
        $this->assertArrayHasKey('displayName', $firstBucket);
        $this->assertNotEquals('', $firstBucket['displayName']);
        $this->assertArrayHasKey('created', $firstBucket);
        $this->assertArrayHasKey('uri', $firstBucket);
        $this->assertArrayHasKey('tables', $firstBucket);
        $this->assertArrayHasKey('created', $firstBucket);
        $this->assertArrayHasKey('lastChangeDate', $firstBucket);
        $this->assertArrayHasKey('updated', $firstBucket);
        $this->assertArrayHasKey('idBranch', $firstBucket);
        $this->assertArrayHasKey('stage', $firstBucket);
        $this->assertSame('in', $firstBucket['stage']);
        $this->assertArrayHasKey('description', $firstBucket);
        $this->assertArrayHasKey('dataSizeBytes', $firstBucket);
        $this->assertArrayHasKey('rowsCount', $firstBucket);
        $this->assertArrayHasKey('backend', $firstBucket);
        $this->assertArrayHasKey('sharing', $firstBucket);
        $this->assertArrayHasKey('databaseName', $firstBucket);
        $this->assertArrayHasKey('path', $firstBucket);
        $this->assertArrayHasKey('color', $firstBucket);
        $this->assertArrayHasKey('owner', $firstBucket);
        $this->assertArrayHasKey('backendPath', $firstBucket);
        $this->assertFalse($firstBucket['isReadOnly']);
        $this->assertFalse($firstBucket['isMaintenance']);
        $this->assertFalse($firstBucket['hasExternalSchema']);
        $this->assertFalse($firstBucket['isSnowflakeSharedDatabase']);

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

        //@phpstan-ignore-next-line
        $this->assertBucketBackendPath($firstBucket);
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
        $bucketId = $this->_testClient->createBucket(name: $bucketName, stage: self::STAGE_IN, color: '#00FF00');

        $bucket = $this->_testClient->getBucket($bucketId);
        //@phpstan-ignore-next-line
        $this->assertBucketBackendPath($bucket);
        $this->assertEquals($branch['id'], $bucket['idBranch']);

        $this->assertEquals($tokenData['owner']['defaultBackend'], $bucket['backend']);
        $this->assertNotEquals($displayName, $bucket['displayName']);
        $this->assertEquals('#00FF00', $bucket['color']);

        $this->assertArrayHasKey('metadata', $bucket);
        $this->assertSame([], $bucket['metadata']);
        // put metadata and test presence
        $metadataApi = new Metadata($this->_testClient);
        $metadataApi->postBucketMetadata($bucket['id'], 'storage-php-client-test', [
            [
                'key' => 'test-key',
                'value' => 'test-value',
            ],
        ]);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertCount(1, $bucket['metadata']);
        $this->assertMetadataEquals(
            [
                'key' => 'test-key',
                'value' => 'test-value',
                'provider' => 'storage-php-client-test',
            ],
            $bucket['metadata'][0],
        );

        $asyncBucketDisplayName = $displayName . '-async';
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $asyncBucketDisplayName, true);
        $bucketUpdateOptions->setColor('red');
        $this->_testClient->updateBucket($bucketUpdateOptions);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEquals($asyncBucketDisplayName, $bucket['displayName']);
        $this->assertEquals('red', $bucket['color']);

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucketUpdateOptions->deleteColor();
        $bucket = $this->_testClient->updateBucket($bucketUpdateOptions);
        //@phpstan-ignore-next-line
        $this->assertBucketBackendPath($bucket);
        try {
            $this->_testClient->createBucket($displayName, self::STAGE_IN);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('The display name "Romanov-Bucket" already exists in project.', $e->getMessage());
        }

        $this->assertEquals($displayName, $bucket['displayName']);
        $this->assertEquals(null, $bucket['color']);

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
            $this->assertEquals('Invalid data - displayName: \'$$$$$\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEquals($displayName, $bucket['displayName']);

        // renaming bucket to the same name should be successful
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucket = $this->_testClient->updateBucket($bucketUpdateOptions);

        // create table and test include parameters
        $tableId = $this->_testClient->createTableAsync(
            $bucket['id'],
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv'),
        );
        $metadataApi->postTableMetadataWithColumns(new TableMetadataUpdateOptions(
            $tableId,
            'test',
            [
                [
                    'key' => 'test-key',
                    'value' => 'test-value',
                ],
            ],
            [
                'id' => [
                    [
                        'key' => 'test-key',
                        'value' => 'test-value',
                    ],
                ],
            ],
        ));

        //test detail
        $bucket = $this->_testClient->getBucket(
            new BucketDetailOptions(
                $bucket['id'],
                ['metadata'],
            ),
        );
        $this->assertBucketWithMetadata($bucket);

        $bucket = $this->_testClient->getBucket(
            new BucketDetailOptions(
                $bucket['id'],
                [
                    'metadata',
                    'columns',
                ],
            ),
        );
        $this->assertBucketWithColumns($bucket);

        $bucket = $this->_testClient->getBucket(
            new BucketDetailOptions(
                $bucket['id'],
                [
                    'metadata',
                    'columnMetadata',
                ],
            ),
        );
        $this->assertBucketWithColumnMetadata($bucket);

        $this->_testClient->dropTable($tableId);
        $this->_testClient->dropBucket($bucket['id']);
    }


    /**
     * @param array{
     *     backendPath: string[],
     *     backend: string,
     *     path: string,
     * } $bucket
     */
    private function assertBucketBackendPath(array $bucket): void
    {
        if ($bucket['backend'] === self::BACKEND_SNOWFLAKE) {
            $projectId = $this->getProjectId($this->_testClient);
            $this->assertCount(2, $bucket['backendPath']);
            $this->assertStringContainsString((string) $projectId, $bucket['backendPath'][0]);
            $this->assertSame($bucket['path'], $bucket['backendPath'][1]);
        } elseif ($bucket['backend'] === self::BACKEND_BIGQUERY) {
            $this->assertCount(1, $bucket['backendPath']);
            $this->assertSame($bucket['path'], $bucket['backendPath'][0]);
        } else {
            $this->fail('Unknown backend ' . $bucket['backend']);
        }
    }

    private function assertBucketWithColumnMetadata(array $bucket): void
    {
        self::assertArrayHasKey('tables', $bucket);
        self::assertCount(1, $bucket['tables']);
        self::assertArrayNotHasKey('columns', $bucket['tables'][0]);
        self::assertArrayNotHasKey('bucket', $bucket['tables'][0]);
        self::assertArrayHasKey('columnMetadata', $bucket['tables'][0]);
        self::assertCount(1, $bucket['tables'][0]['columnMetadata']);
        self::assertArrayHasKey('id', $bucket['tables'][0]['columnMetadata']);
        self::assertCount(1, $bucket['tables'][0]['columnMetadata']['id']);
        self::assertArrayHasKey('key', $bucket['tables'][0]['columnMetadata']['id'][0]);
        self::assertEquals('test-key', $bucket['tables'][0]['columnMetadata']['id'][0]['key']);
        self::assertArrayHasKey('value', $bucket['tables'][0]['columnMetadata']['id'][0]);
        self::assertEquals('test-value', $bucket['tables'][0]['columnMetadata']['id'][0]['value']);
    }

    private function assertBucketWithColumns(array $bucket): void
    {
        self::assertArrayHasKey('tables', $bucket);
        self::assertCount(1, $bucket['tables']);
        self::assertArrayHasKey('columns', $bucket['tables'][0]);
        self::assertCount(2, $bucket['tables'][0]['columns']);
        self::assertArrayNotHasKey('columnMetadata', $bucket['tables'][0]);
        self::assertArrayNotHasKey('bucket', $bucket['tables'][0]);
    }

    private function assertBucketWithMetadata(array $bucket): void
    {
        self::assertArrayHasKey('metadata', $bucket);
        self::assertCount(1, $bucket['metadata']);
        self::assertArrayHasKey('key', $bucket['metadata'][0]);
        self::assertEquals('test-key', $bucket['metadata'][0]['key']);
        self::assertArrayHasKey('value', $bucket['metadata'][0]);
        self::assertEquals('test-value', $bucket['metadata'][0]['value']);

        self::assertArrayHasKey('tables', $bucket);
        self::assertCount(1, $bucket['tables']);
        self::assertArrayNotHasKey('columns', $bucket['tables'][0]);
        self::assertArrayNotHasKey('columnMetadata', $bucket['tables'][0]);
        self::assertArrayNotHasKey('bucket', $bucket['tables'][0]);
        self::assertArrayHasKey('metadata', $bucket['tables'][0]);
        self::assertCount(1, $bucket['tables'][0]['metadata']);
        self::assertArrayHasKey('key', $bucket['tables'][0]['metadata'][0]);
        self::assertEquals('test-key', $bucket['tables'][0]['metadata'][0]['key']);
        self::assertArrayHasKey('value', $bucket['tables'][0]['metadata'][0]);
        self::assertEquals('test-value', $bucket['tables'][0]['metadata'][0]['value']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     * @group SOX-66
     */
    public function testBucketEvents(string $devBranchType, string $userRole): void
    {
        $bucketName = sha1($this->generateDescriptionForTestObject()) . '-test-events';
        $bucketStringId = 'in.c-' . $bucketName;
        $this->dropBucketIfExists($this->_testClient, $bucketStringId, true);
        $bucketId = $this->_testClient->createBucket($bucketName, self::STAGE_IN);
        $this->initEvents($this->_testClient);

        // create bucket event
        $this->_testClient->listTables($bucketId);

        $assertCallback = function ($events) use ($devBranchType, $bucketId) {
            $this->assertCount(1, $events);
            // check bucket events
            $this->assertSame('storage.tablesListed', $events[0]['event']);
            $this->assertSame('Listed tables', $events[0]['message']);
            $this->assertSame($bucketId, $events[0]['objectId']);
            $this->assertSame('bucket', $events[0]['objectType']);
            if ($devBranchType === ClientProvider::DEFAULT_BRANCH) {
                $this->assertSame($this->getDefaultBranchId($this), $events[0]['idBranch']);
            } else {
                assert($this->_testClient instanceof BranchAwareClient);
                $this->assertSame($this->_testClient->getCurrentBranchId(), $events[0]['idBranch']);
            }
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.tablesListed')
            ->setTokenId($this->tokenId)
            ->setObjectId($bucketId);
        $this->assertEventWithRetries($this->_testClient, $assertCallback, $query);

        $bucketEvents = $this->_testClient->listBucketEvents($bucketId);
        $this->assertGreaterThan(2, $bucketEvents);
        $this->assertSame('storage.tablesListed', $bucketEvents[0]['event']);
        $this->assertSame('storage.bucketCreated', $bucketEvents[1]['event']);
        $this->assertNotEmpty($bucketEvents[0]['idBranch']);
        $this->assertNotEmpty($bucketEvents[1]['idBranch']);
        $this->assertSame($bucketStringId, $bucketEvents[0]['objectId']);
        $this->assertSame($bucketStringId, $bucketEvents[1]['objectId']);
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
        $this->assertNotEmpty($buckets, 'There should be at least one bucket prepared for the testcase');

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
            $this->_testClient->dropBucket($newBucketId);
            $this->fail('Should throw exception');
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }

        $this->_testClient->dropBucket($newBucketId, ['force' => true]);

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

        $this->_testClient->dropBucket($newBucketId, ['force' => true]);

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
            $this->assertEquals('Invalid request:
 - displayName: "\'$$$$$\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed."', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }

        $this->_testClient->dropBucket($newBucket['id']);

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
        $this->_testClient->dropBucket($bucket['id']);
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

    /**
     * @group noSOX
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testBucketOwner(string $devBranchType, string $userRole): void
    {
        $token = $this->_testClient->verifyToken();

        $ownerId = $token['adminOwner']['id'];
        $ownerName = $token['adminOwner']['name'];
        $ownerEmail = $token['adminOwner']['email'];

        $this->initEvents($this->_testClient);

        $bucketName = 'bucketOwnerTesting';

        $this->dropBucketIfExists($this->_testClient, "in.c-{$bucketName}", true);
        $bucketId = $this->_testClient->createBucket($bucketName, self::STAGE_IN);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertNull($bucket['owner']);

        try {
            $this->_testClient->bucketOwner($bucketId);
            $this->fail('Should fail.');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('storage.bucket.ownerNotFound', $e->getStringCode());
            $this->assertSame("Owner for bucket in.c-{$bucketName} in project {$token['owner']['id']} not found.", $e->getMessage());
        }

        $this->assertNull($bucket['updated']);

        try {
            $this->_testClient->updateBucketOwner($bucketId, new BucketOwnerUpdateOptions(id: 99999999));
            $this->fail('Should fail.');
        } catch (ClientException $e) {
            $this->assertEquals(404, $e->getCode());
            $this->assertEquals('storage.bucket.ownerNotFound', $e->getStringCode());
            $this->assertEquals("Owner 99999999 for bucket in.c-{$bucketName} in project {$token['owner']['id']} not found.", $e->getMessage());
        }

        $this->_testClient->updateBucketOwner($bucketId, new BucketOwnerUpdateOptions(id: $ownerId));

        $eventAssertCallback = function ($events) use ($bucketId, $ownerId, $ownerName, $ownerEmail) {
            $this->assertCount(1, $events);

            $this->assertEquals('bucket', $events[0]['objectType']);
            $this->assertEquals($bucketId, $events[0]['objectId']);
            $this->assertEquals($ownerId, $events[0]['params']['owner']['id']);
            $this->assertEquals($ownerName, $events[0]['params']['owner']['name']);
            $this->assertEquals($ownerEmail, $events[0]['params']['owner']['email']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketUpdated')
            ->setTokenId($this->tokenId)
            ->setObjectId($bucketId);
        $this->assertEventWithRetries($this->_testClient, $eventAssertCallback, $query);

        $bucketOwner = $this->_testClient->bucketOwner($bucketId);

        $this->assertEquals($ownerId, $bucketOwner['id']);
        $this->assertEquals($ownerName, $bucketOwner['name']);
        $this->assertEquals($ownerEmail, $bucketOwner['email']);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertEquals($ownerId, $bucket['owner']['id']);
        $this->assertEquals($ownerName, $bucket['owner']['name']);
        $this->assertEquals($ownerEmail, $bucket['owner']['email']);
        $this->assertNotNull($bucket['updated']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testBucketDisplayNameUpdate(): void
    {
        $this->initEvents($this->_testClient);

        $bucketName = 'bucketDisplayNameTesting';

        $this->dropBucketIfExists($this->_testClient, "in.c-{$bucketName}", true);
        $bucketId = $this->_testClient->createBucket($bucketName, self::STAGE_IN);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertNull($bucket['updated']);

        $this->_testClient->updateBucket(new BucketUpdateOptions($bucketId, 'newBucketDisplayNameTesting'));

        $eventAssertCallback = function ($events) use ($bucketId) {
            $this->assertCount(1, $events);

            $this->assertSame('bucket', $events[0]['objectType']);
            $this->assertSame($bucketId, $events[0]['objectId']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketUpdated')
            ->setTokenId($this->tokenId)
            ->setObjectId($bucketId);
        $this->assertEventWithRetries($this->_testClient, $eventAssertCallback, $query);

        // update displayName do not change updated column
        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertNull($bucket['updated']);
    }

    /**
     * @dataProvider provideComponentsClientTypeBasedOnSuite
     */
    public function testBucketDescriptionMetadataUpdate(): void
    {
        $this->initEvents($this->_testClient);

        $bucketName = 'bucketMetadataDescriptionTesting';
        $metadataProvider = 'user';
        $descriptionKey = 'KBC.description';

        $this->dropBucketIfExists($this->_testClient, "in.c-{$bucketName}", true);
        $bucketId = $this->_testClient->createBucket($bucketName, self::STAGE_IN);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertNull($bucket['updated']);

        $metadataClient = new Metadata($this->_testClient);

        $bucketMetadata = $metadataClient->listBucketMetadata($bucketId);
        $this->assertEmpty($bucketMetadata);

        $metadataClient->postBucketMetadata($bucketId, $metadataProvider, [[ 'key' => $descriptionKey, 'value' => 'Testing bucket description']]);

        $eventAssertCallback = function ($events) use ($bucketId) {
            $this->assertCount(1, $events);

            $this->assertSame('bucket', $events[0]['objectType']);
            $this->assertSame($bucketId, $events[0]['objectId']);
        };

        $query = new EventsQueryBuilder();
        $query->setEvent('storage.bucketMetadataSet')
            ->setTokenId($this->tokenId)
            ->setObjectId($bucketId);
        $this->assertEventWithRetries($this->_testClient, $eventAssertCallback, $query);

        $bucketMetadata = $metadataClient->listBucketMetadata($bucketId);
        $this->assertNotEmpty($bucketMetadata);

        $bucket = $this->_testClient->getBucket($bucketId);
        $this->assertNotNull($bucket['updated']);
    }

    public function provideComponentsClientTypeBasedOnSuite(): array
    {
        return (new TestSetupHelper())->provideComponentsClientTypeBasedOnSuite($this);
    }
}
