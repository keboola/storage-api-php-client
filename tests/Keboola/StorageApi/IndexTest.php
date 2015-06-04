<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */


use Keboola\StorageApi\Client;

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_IndexTest extends StorageApiTestCase
{

	public function testIndex()
	{
		$index = $this->_client->indexAction();
		$this->assertEquals('storage', $index['api']);
		$this->assertEquals('v2', $index['version']);
		$this->assertArrayHasKey('revision', $index);

		$this->assertInternalType('array', $index['components']);

		$component = reset($index['components']);
		$this->assertArrayHasKey('id', $component);
		$this->assertArrayHasKey('uri', $component);
	}

}