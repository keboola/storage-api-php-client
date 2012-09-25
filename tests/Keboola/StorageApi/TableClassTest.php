<?php
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


	public function setUp()
	{
		parent::setUp();

		$this->_outBucketId = $this->_initEmptyBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');
	}

	public function testSetFromArray()
	{
		$tableId = 'in.api-tests.table';

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

		$table = new \Keboola\StorageApi\Table($this->_client, $tableId);

		$table->setHeader($header);
		$table->setFromArray($data);

		$this->assertEquals($tableId, $table->getId());
		$this->assertEquals('table', $table->getName());
		$this->assertEquals('in.api-tests', $table->getBucketId());
		$this->assertNotEmpty($table->getData());
		$this->assertNotEmpty($table->getHeader());

		$table->setFromArray($dataWithHeader);

		$this->assertEquals($tableId, $table->getId());
		$this->assertEquals('table', $table->getName());
		$this->assertEquals('in.api-tests', $table->getBucketId());
		$this->assertNotEmpty($table->getData());
		$this->assertNotEmpty($table->getHeader());
	}

}
