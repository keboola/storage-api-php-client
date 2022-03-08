<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Event;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\BucketUpdateOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class BucketsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testBucketsList()
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
        $this->assertArrayHasKey('attributes', $firstBucket);
        $this->assertArrayHasKey('displayName', $firstBucket);
        $this->assertNotEquals('', $firstBucket['displayName']);
    }

    public function testBucketDetail()
    {
        $displayName = "Romanov-Bucket";
        $bucketName = 'BucketsTest_testBucketDetail';

        $tokenData = $this->_client->verifyToken();
        $this->dropBucketIfExists($this->_client, 'in.c-' . $bucketName);
        $bucketId = $this->_client->createBucket($bucketName, self::STAGE_IN);

        $bucket = $this->_client->getBucket($bucketId);

        $this->assertFalse($bucket['directAccessEnabled']);
        $this->assertNull($bucket['directAccessSchemaName']);

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
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertSame('The display name "Romanov-Bucket" already exists in project.', $e->getMessage());
        };

        $this->assertEquals($displayName, $bucket['displayName']);

        $bucketUpdateOptions = new BucketUpdateOptions($this->getTestBucketId(), $displayName);
        try {
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('The display name already exists in project');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('The display name "' . $displayName . '" already exists in project.', $e->getMessage());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, '$$$$$');
        try {
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('Wrong display name');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEquals($displayName, $bucket['displayName']);

        // renaming bucket to the same name should be successful
        $bucketUpdateOptions = new BucketUpdateOptions($bucketId, $displayName);
        $bucket = $this->_client->updateBucket($bucketUpdateOptions);

        $this->_client->dropBucket($bucket['id']);
    }

    /**
     * @return int
     */
    private function getLastEventId()
    {
        $lastEvents = $this->_client->listEvents(['limit' => 1]);
        if (!$lastEvents || !is_array($lastEvents)) {
            $this->fail('Cannot get last event ID.');
        }
        $lastEventId = $lastEvents[0]['id'];
        $this->assertIsInt($lastEventId);
        return $lastEventId;
    }

    public function testBucketEvents()
    {
        $lastEventId = $this->getLastEventId();

        $description = 'testBucketEvents';
        $bucketId = $this->initEmptyBucket($this->getTestBucketName($description), self::STAGE_IN, $description);
        $this->assertIsString($bucketId);

        // create dummy event
        $event = new Event();
        $event->setComponent('dummy')
            ->setMessage('bucket sample event');
        $event = $this->createAndWaitForEvent($event);

        // check bucket events
        $events = $this->_client->listBucketEvents($bucketId, ['sinceId' => $lastEventId]);
        $this->assertIsArray($events);
        $this->assertCount(1, (array) $events);

        // check dummy event is not among bucket events
        $this->assertArrayNotHasKey($event['id'], (array) $events);

        $this->_client->dropBucket((string) $bucketId);
    }

    public function testBucketsListWithIncludeParameter()
    {
        $buckets = $this->_client->listBuckets(array(
            'include' => '',
        ));

        $firstBucket = reset($buckets);
        $this->assertArrayNotHasKey('attributes', $firstBucket);
    }

    public function testBucketsListWithIncludeMetadata()
    {
        $buckets = $this->_client->listBuckets(array(
            'include' => 'metadata',
        ));
        $firstBucket = array_filter($buckets, function ($bucket) {
            return $bucket['id'] === $this->_bucketIds[self::STAGE_IN];
        });

        $firstBucket = reset($firstBucket);

        self::assertArrayNotHasKey('attributes', $firstBucket);
        self::assertArrayHasKey('metadata', $firstBucket);
        self::assertEmpty($firstBucket['metadata']);

        $metadataApi = new Metadata($this->_client);
        $metadataApi->postBucketMetadata($firstBucket['id'], 'storage-php-client-test', [
            [
                'key' => 'test-key',
                'value' => 'test-value'
            ]
        ]);

        $buckets = $this->_client->listBuckets(array(
            'include' => 'metadata',
        ));

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

    public function testBucketCreateWithInvalidBackend()
    {
        try {
            $this->_client->createBucket('unknown-backend', 'in', 'desc', 'redshit');
            $this->fail('Exception should be thrown');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }
    }

    public function testBucketManipulation()
    {
        $tokenData = $this->_client->verifyToken();

        if ($tokenData['owner']['defaultBackend'] === self::BACKEND_TERADATA) {
            $this->markTestSkipped('Allow when create table for TD is implemented.');
        }

        $bucketData = [
            'name' => 'test',
            'displayName' => 'test-display-name',
            'stage' => 'in',
            'description' => 'this is just a test',
        ];

        $testBucketId = $bucketData['stage'] . '.c-'.$bucketData['name'];

        $this->dropBucketIfExists($this->_client, $testBucketId);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName']
        );

        $importFile = __DIR__ . '/../../_data/languages.csv';
        // create and import data into source table
        $sourceTableId = $this->_client->createTable(
            $newBucketId,
            'languages',
            new CsvFile($importFile)
        );

        try {
            $this->_client->dropBucket($newBucketId);
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }
        try {
            $this->_client->dropBucket($newBucketId, ['async' => true]);
        } catch (ClientException $e) {
            $this->assertSame('Only empty buckets can be deleted. There are 1 tables in the bucket.', $e->getMessage());
            $this->assertSame('buckets.deleteNotEmpty', $e->getStringCode());
        }

        $this->_client->dropBucket($newBucketId, ['force' => true]);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName']
        );

        $importFile = __DIR__ . '/../../_data/languages.csv';
        // create and import data into source table
        $sourceTableId = $this->_client->createTable(
            $newBucketId,
            'languages',
            new CsvFile($importFile)
        );

        $this->_client->dropBucket($newBucketId, ['async' => true, 'force' => true]);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description'],
            null,
            $bucketData['displayName']
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
                $bucketData['displayName']
            );
            $this->fail('Display name already exist for project');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('The display name "test-display-name" already exists in project.', $e->getMessage());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
        }

        try {
            $this->_client->createBucket(
                $bucketData['name'] . '-' . time(),
                $bucketData['stage'],
                $bucketData['description'],
                null,
                '$$$$$'
            );
            $this->fail('Display name provided is invalid');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
        }

        $this->_client->dropBucket($newBucket['id']);

        $newBucketId = $this->_client->createBucket(
            $bucketData['name'],
            $bucketData['stage'],
            $bucketData['description']
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

    public function testBucketCreateWithoutDescription()
    {
        $bucketId = $this->_client->createBucket('something', self::STAGE_IN);
        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEmpty($bucket['description']);
        $this->_client->dropBucket($bucket['id']);
    }

    public function testBucketAttributes()
    {
        $bucketId = $this->getTestBucketId();

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertEmpty($bucket['attributes'], 'empty attributes');


        // create
        $this->_client->setBucketAttribute($bucketId, 's', 'lala');
        $this->_client->setBucketAttribute($bucketId, 'other', 'hello', true);
        $bucket = $this->_client->getBucket($bucketId);
        $this->assertArrayEqualsSorted(array(
            array(
                'name' => 's',
                'value' => 'lala',
                'protected' => false,
            ),
            array(
                'name' => 'other',
                'value' => 'hello',
                'protected' => true,
            ),
        ), $bucket['attributes'], 'name', 'attribute set');

        // update
        $this->_client->setBucketAttribute($bucketId, 's', 'papa');
        $bucket = $this->_client->getBucket($bucketId);
        $this->assertArrayEqualsSorted($bucket['attributes'], array(
            array(
                'name' => 's',
                'value' => 'papa',
                'protected' => false,
            ),
            array(
                'name' => 'other',
                'value' => 'hello',
                'protected' => true,
            ),
        ), 'name', 'attribute update');

        // delete
        $this->_client->deleteBucketAttribute($bucketId, 's');
        $bucket = $this->_client->getBucket($bucketId);
        $this->assertArrayEqualsSorted($bucket['attributes'], array(
            array(
                'name' => 'other',
                'value' => 'hello',
                'protected' => true,
            )
        ), 'name', 'attribute delete');

        $this->_client->deleteBucketAttribute($bucketId, 'other');
    }

    public function testBucketExists()
    {
        $this->assertTrue($this->_client->bucketExists($this->getTestBucketId()));
        $this->assertFalse($this->_client->bucketExists('in.ukulele'));
    }

    public function testBucketAttributesReplace()
    {
        $bucketId = $this->getTestBucketId();
        $this->clearBucketAttributes($bucketId);
        $this->_client->setBucketAttribute($bucketId, 'first', 'something');

        $newAttributes = array(
            array(
                'name' => 'new',
                'value' => 'new',
            ),
            array(
                'name' => 'second',
                'value' => 'second value',
                'protected' => true,
            ),
        );
        $this->_client->replaceBucketAttributes($bucketId, $newAttributes);

        $bucket = $this->_client->getBucket($bucketId);
        $this->assertCount(count($newAttributes), $bucket['attributes']);

        $this->assertEquals($newAttributes[0]['name'], $bucket['attributes'][0]['name']);
        $this->assertEquals($newAttributes[0]['value'], $bucket['attributes'][0]['value']);
        $this->assertFalse($bucket['attributes'][0]['protected']);
    }

    public function testBucketAttributesClear()
    {
        $bucketId = $this->getTestBucketId();
        $this->clearBucketAttributes($bucketId);

        $this->_client->replaceBucketAttributes($bucketId);
        $bucket = $this->_client->getBucket($bucketId);

        $this->assertEmpty($bucket['attributes']);
    }

    /**
     * @param $attributes
     * @dataProvider invalidAttributes
     */
    public function testBucketAttributesReplaceValidation($attributes)
    {
        $bucketId = $this->getTestBucketId();
        $this->clearBucketAttributes($bucketId);

        try {
            $this->_client->replaceBucketAttributes($bucketId, $attributes);
            $this->fail('Attributes should be invalid');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.attributes.validation', $e->getStringCode());
        }
    }

    public function invalidAttributes()
    {
        return array(
            array(
                array(
                    array(
                        'nome' => 'ukulele',
                    ),
                    array(
                        'name' => 'jehovista',
                    ),
                ),
            )
        );
    }


    private function clearBucketAttributes($bucketId)
    {
        $bucket = $this->_client->getBucket($bucketId);

        foreach ($bucket['attributes'] as $attribute) {
            $this->_client->deleteBucketAttribute($bucketId, $attribute['name']);
        }
    }
}
