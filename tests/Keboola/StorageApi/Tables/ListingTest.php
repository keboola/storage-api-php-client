<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:30
 * To change this template use File | Settings | File Templates.
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_ListingTest extends StorageApiTestCase
{

	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	public function testTableExists()
	{
		$this->assertFalse($this->_client->tableExists($this->getTestBucketId() . '.languages'));

		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);
		$this->assertTrue($this->_client->tableExists($tableId));
	}

	public function testListTables()
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$this->_client->setTableAttribute($tableId, 'test', 'something');
		$tables = $this->_client->listTables($this->getTestBucketId());

		$this->assertCount(1, $tables);

		$firstTable = reset($tables);
		$this->assertArrayHasKey('attributes', $firstTable, 'List bucket tables are returned with attributes');
		$this->assertCount(1, $firstTable['attributes']);

		$tables = $this->_client->listTables();
		$firstTable = false;
		foreach ($tables as $table) {
			if ($table['id'] != $tableId) {
				continue;
			}
			$firstTable = $table;
			break;
		}

		$this->assertArrayHasKey('attributes', $firstTable, 'List tables are returned with attributes');
		$this->assertCount(1, $firstTable['attributes']);
		$this->assertArrayHasKey('bucket', $firstTable, 'List tables are returned with attributes');
		$this->assertArrayNotHasKey('columns', $firstTable);
	}

	public function testListTablesWithIncludeParam()
	{
		$this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$tables = $this->_client->listTables($this->getTestBucketId(), array(
			'include' => '', // don't include anything
		));

		$firstTable = reset($tables);
		$this->assertArrayNotHasKey('attributes', $firstTable);
		$this->assertArrayNotHasKey('bucket', $firstTable);

		$tables = $this->_client->listTables(null, array(
			'include' => '', // don't include anything
		));

		$firstTable = reset($tables);
		$this->assertArrayNotHasKey('attributes', $firstTable);
		$this->assertArrayNotHasKey('bucket', $firstTable);

	}

	public function testListTablesWithColumns()
	{
		$this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

		$tables = $this->_client->listTables($this->getTestBucketId(), array(
			'include' => 'columns',
		));

		$firstTable = reset($tables);
		$this->assertArrayHasKey('columns', $firstTable);
		$this->assertEquals(array('id', 'name'), $firstTable['columns']);

		$tables = $this->_client->listTables(null, array(
			'include' => 'columns',
		));

		$firstTable = reset($tables);
		$this->assertArrayHasKey('columns', $firstTable);
		$this->assertEquals(array('id', 'name'), $firstTable['columns']);
	}

	public function testTableAttributes()
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

		$table = $this->_client->getTable($tableId);
		$this->assertEmpty($table['attributes'], 'empty attributes after table create');

		// create
		$this->_client->setTableAttribute($tableId, 's', 'lala');
		$this->_client->setTableAttribute($tableId, 'other', 'hello', true);
		$table = $this->_client->getTable($tableId);


		$this->assertArrayEqualsSorted($table['attributes'], array(
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
		), 'name', 'attribute set');

		// update
		$this->_client->setTableAttribute($tableId, 's', 'papa');
		$table = $this->_client->getTable($tableId);
		$this->assertArrayEqualsSorted($table['attributes'], array(
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
		$this->_client->deleteTableAttribute($tableId, 's');
		$table = $this->_client->getTable($tableId);
		$this->assertArrayEqualsSorted($table['attributes'], array(
			array(
				'name' => 'other',
				'value' => 'hello',
				'protected' => true,
			),
		), 'attribute delete');

		$this->_client->deleteTableAttribute($tableId, 'other');
	}

	public function testTableAttributesReplace()
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$this->_client->setTableAttribute($tableId, 'first', 'something');

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
		$this->_client->replaceTableAttributes($tableId, $newAttributes);

		$table = $this->_client->getTable($tableId);
		$this->assertCount(count($newAttributes), $table['attributes']);

		$this->assertEquals($newAttributes[0]['name'], $table['attributes'][0]['name']);
		$this->assertEquals($newAttributes[0]['value'], $table['attributes'][0]['value']);
		$this->assertFalse($table['attributes'][0]['protected']);
	}

	public function testTableAttributesClear()
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$this->_client->setTableAttribute($tableId, 'first', 'something');

		$this->_client->replaceTableAttributes($tableId);
		$table = $this->_client->getTable($tableId);

		$this->assertEmpty($table['attributes']);
	}

	/**
	 * @param $attributes
	 * @dataProvider invalidAttributes
	 */
	public function testTableAttributesReplaceValidation($attributes)
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		try {
			$this->_client->replaceTableAttributes($tableId, $attributes);
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




}