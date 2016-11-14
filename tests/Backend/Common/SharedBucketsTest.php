<?php
/**
 *
 * User: Erik Zigo
 *
 */
namespace Keboola\Test\Backend\Common;

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
        $this->_initEmptyTestBuckets();

        $this->_client2 = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_OTHER_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ));
    }

    protected function _initEmptyTestBuckets()
    {
        parent::_initEmptyTestBuckets();

        foreach ($this->_bucketIds AS $bucketId) {
            if ($this->_client->isSharedBucket($bucketId)) {
                $this->_client->unshareBucket($bucketId);
            }
        }
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
            $this->assertRegExp('/is already shared to organization/ui', $e->getMessage());
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

    public function testSharing()
    {
        $bucketId = reset($this->_bucketIds);

        $this->_client->shareBucket($bucketId);
        $this->assertTrue($this->_client->isSharedBucket($bucketId));

        $response = $this->_client2->listSharedBuckets();
        $this->assertCount(1, $response);

        $sharedBucket = reset($response);

        $this->_client2->linkBucket("Linked bucket", $sharedBucket['project']['id'], $sharedBucket['id']);
    }
}