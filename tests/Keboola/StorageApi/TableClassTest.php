<?php
use Keboola\StorageApi\TableException;

/**
 * Created by JetBrains PhpStorm.
 * User: Miro
 * Date: 25.9.12
 * Time: 14:20
 */

class Keboola_StorageApi_TableClassTest extends StorageApiTestCase
{
	protected $_inBucketId;
	protected $_outBucketId;
	protected $_tableId = 'in.c-api-tests.table';


	public function setUp()
	{
		parent::setUp();

		$this->_outBucketId = $this->_initEmptyBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');
	}

	public function testSetFromArray()
	{
		$header = array('id', 'col1', 'col2', 'col3', 'col4');
		$data = array(
			array('1', 'abc', 'def', 'ghj', 'klm'),
			array('2', 'nop', 'qrs', 'tuv', 'wxyz'),
			array('3', 'abc', 'def', 'ghj', 'klm'),
			array('4', 'nop', 'qrs', 'tuv', 'wxyz'),
		);

		$dataWithHeader = array(
			$header,
			array('1', 'abc', 'def', 'ghj', 'klm'),
			array('2', 'nop', 'qrs', 'tuv', 'wxyz'),
			array('3', 'abc', 'def', 'ghj', 'klm'),
			array('4', 'nop', 'qrs', 'tuv', 'wxyz')
		);

		$table = new \Keboola\StorageApi\Table($this->_client, $this->_tableId);

		$this->assertEquals($this->_tableId, $table->getId());
		$this->assertEquals('table', $table->getName());
		$this->assertEquals('in.c-api-tests', $table->getBucketId());

		$table->setHeader($header);
		$table->setFromArray($data);

		$this->assertNotEmpty($table->getData());
		$this->assertNotEmpty($table->getHeader());
		$this->assertEquals($header, $table->getHeader());
		$this->assertEquals($data, $table->getData());

		$table->setFromArray($dataWithHeader, true);

		$this->assertNotEmpty($table->getData());
		$this->assertNotEmpty($table->getHeader());
		$this->assertEquals($header, $table->getHeader());
		$this->assertEquals($data, $table->getData());
	}

	public function testSave()
	{
		$data = array(
			array('id', 'col1', 'col2', 'col3', 'col4'),
			array('1', 'abc', 'def', 'ghj', 'klm'),
			array('2', 'nop', 'qrs', 'tuv', 'wxyz'),
			array('3', 'abc', 'def', 'ghj', 'klm'),
			array('4', 'nop', 'qrs', 'tuv', 'wxyz')
		);

		$table = new \Keboola\StorageApi\Table($this->_client, $this->_tableId);
		$table->setFromArray($data, true);
		$table->setAttribute('testAttribute', 'test');
		$table->addIndex('col1');
		$table->addIndex('col2');
		$table->save();

		$result = \Keboola\StorageApi\Table::csvStringToArray($this->_client->exportTable($this->_tableId));

		$this->assertEquals($data, $result, 'data saving to Storage API');
		$this->assertEquals($table->getAttribute('testAttribute'), 'test', 'saving attributes to Storage API');

		$tableInfo = $this->_client->getTable($this->_tableId);
		$this->assertEquals(2, count(array_intersect($tableInfo['indexedColumns'], $table->getIndices())), 'getting indexed columns from Storage API');
	}

	public function testSaveFromFile()
	{
		$data = array(
			array('id', 'col1', 'col2', 'col3', 'col4'),
			array('1', 'abc', 'def', 'ghj', 'klm'),
			array('2', 'nop', 'qrs', 'tuv', 'wxyz'),
			array('3', 'abc', 'def', 'ghj', 'klm'),
			array('4', 'nop', 'qrs', 'tuv', 'wxyz')
		);

		$tempfile = tempnam(__DIR__ . "/tmp/", 'sapi-client-test-table-');
		$file = new \Keboola\Csv\CsvFile($tempfile);
		foreach ($data as $row) {
			$file->writeRow($row);
		}
		unset($file);

		$table = new \Keboola\StorageApi\Table($this->_client, $this->_tableId, $tempfile);
		$table->save(false, true);

		$result = \Keboola\StorageApi\Table::csvStringToArray($this->_client->exportTable($this->_tableId));
		$this->assertEquals($data, $result, 'data saving to Storage API');
	}

	public function testSaveAsync()
	{
		$data = array(
			array('id', 'col1', 'col2', 'col3', 'col4'),
			array('1', 'abc', 'def', 'ghj', 'klm'),
			array('2', 'nop', 'qrs', 'tuv', 'wxyz'),
			array('3', 'abc', 'def', 'ghj', 'klm'),
			array('4', 'nop', 'qrs', 'tuv', 'wxyz')
		);

		$table = new \Keboola\StorageApi\Table($this->_client, $this->_tableId);
		$table->setFromArray($data, true);
		$table->setAttribute('testAttribute', 'test');
		$table->addIndex('col1');
		$table->addIndex('col2');

		// Async set to true
		$table->save($async = true);

		$result = \Keboola\StorageApi\Table::csvStringToArray($this->_client->exportTable($this->_tableId));

		$this->assertEquals($data, $result, 'data saving to Storage API');
		$this->assertEquals($table->getAttribute('testAttribute'), 'test', 'saving attributes to Storage API');

		$tableInfo = $this->_client->getTable($this->_tableId);
		$this->assertEquals(2, count(array_intersect($tableInfo['indexedColumns'], $table->getIndices())), 'getting indexed columns from Storage API');
	}

	public function testEmptyHeaderReplacement()
	{
		$data = array(
			array('', '', '', '', ''),
			array('1', 'abc', 'def', 'ghj', 'klm'),
			array('2', 'nop', 'qrs', 'tuv', 'wxyz'),
			array('3', 'abc', 'def', 'ghj', 'klm'),
			array('4', 'nop', 'qrs', 'tuv', 'wxyz')
		);

		$table = new \Keboola\StorageApi\Table($this->_client, $this->_tableId);
		$table->setFromArray($data, true);

		$testHeader = array('col0', 'col1', 'col2', 'col3', 'col4');

		$this->assertEquals($table->getHeader(), $testHeader);
	}

	public function testInvalidTableName()
	{
		try {
			new \Keboola\StorageApi\Table($this->_client, 'completely-invalid-name');
			$this->fail("Invalid table name should cause exception.");
		} catch (TableException $e) {}
	}

	public function testInvalidBucket()
	{
		try {
			new \Keboola\StorageApi\Table($this->_client, 'in.non-existent-bucket.test-table');
			$this->fail("Non-existent bucket should cause exception.");
		} catch (TableException $e) {}
	}

	//@TODO: Test Exceptions
}
