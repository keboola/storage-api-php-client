<?php

class MetadataTest extends \Keboola\Test\StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyTestBuckets();
		$this->_client->createTable($this->getTestBucketId(), "table", new \Keboola\Csv\CsvFile(__DIR__ . '/../../_data/users.csv'));
	}

	public function testBucketMetadata()
	{
		$bucketId = $this->getTestBucketId();
		$metadataApi = new \Keboola\StorageApi\Metadata($this->_client);
		
		$md = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$md->setFromArray(array(
			"key" => "test_metadata_key1",
			"value" => "testval",
			"provider" => "keboola.storage-api-php-client_test-runner"
		));
		$md2 = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$md2->setFromArray(array(
			"key" => "test_metadata_key2",
			"value" => "testval",
			"provider" => "keboola.storage-api-php-client_test-runner"
		));
		$testMetadata = array($md, $md2);
		
		$metadatas = $metadataApi->postBucketMetadata($bucketId, $testMetadata);
		
		$this->assertEquals(2, count($metadatas));
		$this->assertArrayHasKey("key", $metadatas[0]);
		$this->assertArrayHasKey("value", $metadatas[0]);
		$this->assertArrayHasKey("provider", $metadatas[0]);
		$this->assertArrayHasKey("timestamp", $metadatas[0]);
		$this->assertEquals("keboola.storage-api-php-client_test-runner", $metadatas[0]['provider']);

		$origValue = $metadatas[0]['value'];
		$mdCopy = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$mdCopy->setFromArray($metadatas[0]);
		$mdCopy->setValue("newValue");
		
		$newMetadata = $metadataApi->putBucketMetadata($bucketId, $mdCopy);
		$this->assertEquals($newMetadata['id'], $metadatas[0]['id']);
		$this->assertEquals("newValue", $newMetadata['value']);
		$this->assertGreaterThanOrEqual($newMetadata['timestamp'], $metadatas[0]['timestamp']);

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
		$metadataApi = new \Keboola\StorageApi\Metadata($this->_client);

		$md = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$md->setFromArray(array(
			"key" => "test_metadata_key1",
			"value" => "testval",
			"provider" => "keboola.storage-api-php-client_test-runner"
		));
		$md2 = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$md2->setFromArray(array(
			"key" => "test_metadata_key2",
			"value" => "testval",
			"provider" => "keboola.storage-api-php-client_test-runner"
		));
		$testMetadata = array($md, $md2);

		$metadatas = $metadataApi->postTableMetadata($tableId, $testMetadata);

		$this->assertEquals(2, count($metadatas));
		$this->assertArrayHasKey("key", $metadatas[0]);
		$this->assertArrayHasKey("value", $metadatas[0]);
		$this->assertArrayHasKey("provider", $metadatas[0]);
		$this->assertArrayHasKey("timestamp", $metadatas[0]);
		$this->assertEquals("keboola.storage-api-php-client_test-runner", $metadatas[0]['provider']);

		$mdCopy = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$mdCopy->setFromArray($metadatas[0]);
		$mdCopy->setValue("newValue");

		$newMetadata = $metadataApi->putTableMetadata($tableId, $mdCopy);
		$this->assertEquals($newMetadata['id'], $metadatas[0]['id']);
		$this->assertEquals("newValue", $newMetadata['value']);
		$this->assertGreaterThanOrEqual($newMetadata['timestamp'], $metadatas[0]['timestamp']);

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
		$metadataApi = new \Keboola\StorageApi\Metadata($this->_client);

		$md = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$md->setFromArray(array(
			"key" => "test_metadata_key1",
			"value" => "testval",
			"provider" => "keboola.storage-api-php-client_test-runner"
		));
		$md2 = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$md2->setFromArray(array(
			"key" => "test_metadata_key2",
			"value" => "testval",
			"provider" => "keboola.storage-api-php-client_test-runner"
		));
		$testMetadata = array($md, $md2);

		$metadatas = $metadataApi->postColumnMetadata($columnId, $testMetadata);

		$this->assertEquals(2, count($metadatas));
		$this->assertArrayHasKey("key", $metadatas[0]);
		$this->assertArrayHasKey("value", $metadatas[0]);
		$this->assertArrayHasKey("provider", $metadatas[0]);
		$this->assertArrayHasKey("timestamp", $metadatas[0]);
		$this->assertEquals("keboola.storage-api-php-client_test-runner", $metadatas[0]['provider']);

		$mdCopy = new \Keboola\StorageApi\Options\Metadata\Metadatum();
		$mdCopy->setFromArray($metadatas[0]);
		$mdCopy->setValue("newValue");

		$newMetadata = $metadataApi->putColumnMetadata($columnId, $mdCopy);
		$this->assertEquals($newMetadata['id'], $metadatas[0]['id']);
		$this->assertEquals("newValue", $newMetadata['value']);
		$this->assertGreaterThanOrEqual($newMetadata['timestamp'], $metadatas[0]['timestamp']);

		$metadataApi->deleteColumnMetadata($columnId, $mdCopy['id']);

		$mdList = $metadataApi->listColumnMetadata($columnId);

		$this->assertEquals(1, count($mdList));

		$this->assertEquals($metadatas[1]['key'], $mdList[0]['key']);
		$this->assertEquals($metadatas[1]['value'], $mdList[0]['value']);
		$this->assertEquals($metadatas[1]['provider'], $mdList[0]['provider']);
		$this->assertEquals($metadatas[1]['timestamp'], $mdList[0]['timestamp']);
	}
}