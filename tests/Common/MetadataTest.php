<?php

namespace Keboola\Test\Common;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Metadata;

class MetadataTest extends StorageApiTestCase
{
	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyTestBuckets();
		$this->_client->createTable($this->getTestBucketId(), "table", new CsvFile(__DIR__ . '/../_data/users.csv'));
	}

	public function testBucketMetadata()
	{
		$bucketId = $this->getTestBucketId();
		$metadataApi = new Metadata($this->_client);

		$md = array(
			"key" => "test_metadata_key1",
			"value" => "testval"
		);
		$md2 = array(
			"key" => "test_metadata_key2",
			"value" => "testval"
		);
		$testMetadata = array($md, $md2);

		$provider = "keboola.storage-api-php-client_test-runner";
		$metadatas = $metadataApi->postBucketMetadata($bucketId, $provider, $testMetadata);

		$this->assertEquals(2, count($metadatas));
		$this->assertArrayHasKey("key", $metadatas[0]);
		$this->assertArrayHasKey("value", $metadatas[0]);
		$this->assertArrayHasKey("provider", $metadatas[0]);
		$this->assertArrayHasKey("timestamp", $metadatas[0]);
		$this->assertEquals("keboola.storage-api-php-client_test-runner", $metadatas[0]['provider']);

		$origValue = $metadatas[0]['value'];
		$mdCopy = $metadatas[0];
		$mdCopy['value'] = "newValue";

		$newMetadata = $metadataApi->postBucketMetadata($bucketId, $provider, array($mdCopy));

		foreach ($newMetadata as $metadata) {
			if ($metadata['id'] == $metadatas[0]['id']) {
				$this->assertEquals("newValue", $metadata['value']);
			} else {
				$this->assertEquals("testval", $metadata['value']);
			}
		}

		$metadataApi->deleteBucketMetadata($bucketId, $mdCopy['id']);

		$mdList = $metadataApi->listBucketMetadata($bucketId);

		$this->assertEquals(1, count($mdList));

		$this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
		$this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
		$this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
		$this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);
	}

	public function testTableMetadata()
	{
		$tableId = $this->getTestBucketId() . '.table';
		$metadataApi = new Metadata($this->_client);

		$md = array(
			"key" => "test_metadata_key1",
			"value" => "testval",
		);
		$md2 = array(
			"key" => "test_metadata_key2",
			"value" => "testval",
		);
		$testMetadata = array($md, $md2);

		$provider = "keboola.storage-api-php-client_test-runner";

		$metadatas = $metadataApi->postTableMetadata($tableId, $provider, $testMetadata);

		$this->assertEquals(2, count($metadatas));
		$this->assertArrayHasKey("key", $metadatas[0]);
		$this->assertArrayHasKey("value", $metadatas[0]);
		$this->assertArrayHasKey("provider", $metadatas[0]);
		$this->assertArrayHasKey("timestamp", $metadatas[0]);
		$this->assertEquals("keboola.storage-api-php-client_test-runner", $metadatas[0]['provider']);

		$mdCopy = $metadatas[0];
		$mdCopy['value'] = "newValue";

		$newMetadata = $metadataApi->postTableMetadata($tableId, $provider, array($mdCopy));

		foreach ($newMetadata as $metadata) {
			if ($metadata['id'] == $metadatas[0]['id']) {
				$this->assertEquals("newValue", $metadata['value']);
				$this->assertGreaterThanOrEqual($metadata['timestamp'], $metadatas[0]['timestamp']);
			} else {
				$this->assertEquals("testval", $metadata['value']);
			}
		}

		$metadataApi->deleteTableMetadata($tableId, $mdCopy['id']);

		$mdList = $metadataApi->listTableMetadata($tableId);

		$this->assertEquals(1, count($mdList));

		$this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
		$this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
		$this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
		$this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);
	}


	public function testColumnMetadata()
	{
		$columnId = $this->getTestBucketId() . '.table.id';
		$metadataApi = new Metadata($this->_client);

		$md = array(
			"key" => "test_metadata_key1",
			"value" => "testval",
		);
		$md2 = array(
			"key" => "test_metadata_key2",
			"value" => "testval",
		);
		$testMetadata = array($md, $md2);

		$provider = "keboola.storage-api-php-client_test-runner";

		$metadatas = $metadataApi->postColumnMetadata($columnId, $provider, $testMetadata);

		$this->assertEquals(2, count($metadatas));
		$this->assertArrayHasKey("key", $metadatas[0]);
		$this->assertArrayHasKey("value", $metadatas[0]);
		$this->assertArrayHasKey("provider", $metadatas[0]);
		$this->assertArrayHasKey("timestamp", $metadatas[0]);
		$this->assertEquals("keboola.storage-api-php-client_test-runner", $metadatas[0]['provider']);

		$mdCopy = $metadatas[0];
		$mdCopy['value'] = "newValue";

		$newMetadata = $metadataApi->postColumnMetadata($columnId, $provider, array($mdCopy));
		foreach ($newMetadata as $metadata) {
			if ($metadata['id'] == $metadatas[0]['id']) {
				$this->assertEquals("newValue", $metadata['value']);
				$this->assertGreaterThanOrEqual($metadata['timestamp'], $metadatas[0]['timestamp']);
			} else {
				$this->assertEquals("testval", $metadata['value']);
			}
		}

		$metadataApi->deleteColumnMetadata($columnId, $mdCopy['id']);

		$mdList = $metadataApi->listColumnMetadata($columnId);

		$this->assertEquals(1, count($mdList));

		$this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
		$this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
		$this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
		$this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);
	}
}