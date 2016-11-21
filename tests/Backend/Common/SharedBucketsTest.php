<?php
/**
 *
 * User: Erik Zigo
 *
 */
namespace Keboola\Test\Backend\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class SharedBucketsTest extends StorageApiTestCase
{
    /**
     * @var Client
     */
    private $_client2;

    public function setUp()
    {
        parent::setUp();

        $this->_client2 = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_OTHER_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ));

        $this->_initEmptyTestBuckets();
    }

    protected function _initEmptyTestBuckets()
    {
        // unlink buckets
        foreach ($this->_client2->listBuckets() as $bucket) {
            if ($bucket['isReadOnly']) {
                //@FIXME better linked validation
                $this->_client2->dropBucket($bucket['id']);
            }
        }

        // unshare buckets
        foreach ($this->_client->listBuckets() as $bucket) {
            if ($this->_client->isSharedBucket($bucket['id'])) {
                $this->_client->unshareBucket($bucket['id']);
            }
        }

        parent::_initEmptyTestBuckets();
    }

    public function testShareBucket()
    {
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

    public function testSharedBuckets()
    {
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client->verifyToken();
        $this->assertArrayHasKey('owner', $response);

        $this->assertArrayHasKey('id', $response['owner']);
        $this->assertArrayHasKey('name', $response['owner']);

        $project = $response['owner'];

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        foreach ($response as $sharedBucket) {
            $this->assertArrayHasKey('id', $sharedBucket);
            $this->assertArrayHasKey('description', $sharedBucket);
            $this->assertArrayHasKey('project', $sharedBucket);

            $this->assertArrayHasKey('id', $sharedBucket['project']);
            $this->assertArrayHasKey('name', $sharedBucket['project']);

            $this->assertEquals($sharedBucket['project']['id'], $project['id']);
            $this->assertEquals($sharedBucket['project']['name'], $project['name']);
        }
    }

    public function testLinkBucket()
    {
        $bucketId = reset($this->_bucketIds);
        $sourceBucket = $this->_client->getBucket($bucketId);

        $this->_client->shareBucket($bucketId);

        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $id = $this->_client2->linkBucket("linked-" . time(), $sharedBucket['project']['id'], $sharedBucket['id']);

        $bucket = $this->_client2->getBucket($id);
        $this->assertArrayHasKey('id', $bucket);
        $this->assertArrayHasKey('stage', $bucket);
        $this->assertArrayHasKey('backend', $bucket);
        $this->assertArrayHasKey('description', $bucket);
        $this->assertArrayHasKey('isReadonly', $bucket);

        $this->assertEquals($id, $bucket['id']);
        $this->assertEquals('in', $bucket['stage']);
        $this->assertTrue($bucket['isReadonly']);
        $this->assertEquals($sourceBucket['backend'], $bucket['backend']);
        $this->assertEquals($sourceBucket['description'], $bucket['description']);
    }

    public function testLinkedBucket()
    {
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

        $this->_client->markTableColumnAsIndexed($tableId, 'name');

        $this->_client->shareBucket($bucketId);

        // link
        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $linkedBucketId = $this->_client2->linkBucket(
            "linked-" . time(),
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

        // validate tables
        $fieldNames = [
            'name', 'columns', 'isAlias',
            'primaryKey', 'indexedColumns',
            'name', 'dataSizeBytes', 'rowsCount', //@TODO validate dates too?
        ];

        $tables = $this->_client->listTables($bucketId, ['include' => 'columns']);
        $linkedTables = $this->_client2->listTables($linkedBucketId, ['include' => 'columns']);

        foreach ($tables as $i => $table) {
            foreach ($fieldNames as $fieldName) {
                $this->assertEquals($table[$fieldName], $linkedTables[$i][$fieldName]);
            }

            $data = $this->_client->exportTable($table['id']);
            $linkedData = $this->_client2->exportTable($linkedTables[$i]['id']);

            $this->assertEquals($data, $linkedData);
        }
    }

    public function testUnshareAlreadyLinkedBucket()
    {
        $this->fail();
    }

    public function testLinkedBucketDrop()
    {
        $this->fail();
    }
}