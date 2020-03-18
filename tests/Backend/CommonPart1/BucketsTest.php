<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */
namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\BucketUpdateOptions;
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
        $tokenData = $this->_client->verifyToken();
        $displayName = "Romanov-Bucket";
        $bucket = $this->_client->getBucket($this->getTestBucketId());
        $this->assertEquals($tokenData['owner']['defaultBackend'], $bucket['backend']);

        $this->assertNotEquals($displayName, $bucket['displayName']);

        $bucketUpdateOptions = new BucketUpdateOptions($this->getTestBucketId(), $displayName);
        $bucket = $this->_client->updateBucket($bucketUpdateOptions);

        $this->assertEquals($displayName, $bucket['displayName']);

        try {
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('The display name already exists in project');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('The display name "' . $displayName . '" already exists in project.', $e->getMessage());
            $this->assertEquals('storage.buckets.alreadyExists', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucketUpdateOptions = new BucketUpdateOptions($this->getTestBucketId(), '$$$$$');
        try {
            $this->_client->updateBucket($bucketUpdateOptions);
            $this->fail('Wrong display name');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('Invalid data - displayName: Only alphanumeric characters dash and underscores are allowed.', $e->getMessage());
            $this->assertEquals('storage.buckets.validation', $e->getStringCode());
            $this->assertEquals(400, $e->getCode());
        }

        $bucket = $this->_client->getBucket($this->getTestBucketId());
        $this->assertEquals($displayName, $bucket['displayName']);

        $this->_client->dropBucket($bucket['id']);
    }

    public function testBucketEvents()
    {
        $events = $this->_client->listBucketEvents($this->getTestBucketId());
        $this->assertNotEmpty($events);
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

        $firstBucket = reset($buckets);
        $this->assertArrayNotHasKey('attributes', $firstBucket);
        $this->assertArrayHasKey('metadata', $firstBucket);
        $this->assertEmpty($firstBucket['metadata']);

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
        $firstBucket = reset($buckets);
        $this->assertArrayHasKey('metadata', $firstBucket);
        $this->assertCount(1, $firstBucket['metadata']);
        $this->assertArrayHasKey('key', $firstBucket['metadata'][0]);
        $this->assertEquals('test-key', $firstBucket['metadata'][0]['key']);
        $this->assertArrayHasKey('value', $firstBucket['metadata'][0]);
        $this->assertEquals('test-value', $firstBucket['metadata'][0]['value']);
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

        $bucketData = [
            'name' => 'test',
            'displayName' => 'test-display-name',
            'stage' => 'in',
            'description' => 'this is just a test',
        ];

        $testBucketId = $bucketData['stage'] . '.c-'.$bucketData['name'];

        if ($this->_client->getBucket($testBucketId)) {
            $this->_client->dropBucket($testBucketId);
        }

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
