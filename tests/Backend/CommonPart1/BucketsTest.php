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
use Keboola\Test\StorageApiTestCase;

class BucketsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testBucketsListWithIncludeMetadata()
    {
        $buckets = $this->_client->listBuckets([
            'include' => 'metadata',
        ]);

        $firstBucket = reset($buckets);
        $this->assertArrayNotHasKey('attributes', $firstBucket);
        $this->assertArrayHasKey('metadata', $firstBucket);
        $this->assertEmpty($firstBucket['metadata']);

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
        $firstBucket = reset($buckets);
        $this->assertArrayHasKey('metadata', $firstBucket);
        $this->assertCount(1, $firstBucket['metadata']);
        $this->assertArrayHasKey('key', $firstBucket['metadata'][0]);
        $this->assertEquals('test-key', $firstBucket['metadata'][0]['key']);
        $this->assertArrayHasKey('value', $firstBucket['metadata'][0]);
        $this->assertEquals('test-value', $firstBucket['metadata'][0]['value']);
    }
}
