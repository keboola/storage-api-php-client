<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_CreateTest extends StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	/**
	 * @dataProvider tableCreateData
	 * @param $createFile
	 */
	public function testTableCreate($tableName, $createFile, $expectationFile, $async, $backend, $options = array())
	{
		$createMethod = $async ? 'createTableAsync' : 'createTable';
		$tableId = $this->_client->{$createMethod}(
			$this->getTestBucketId(self::STAGE_IN, $backend),
			$tableName,
			new CsvFile($createFile),
			$options
		);
		$table = $this->_client->getTable($tableId);

		$expectationFileCsv = new CsvFile($expectationFile);

		$this->assertEquals($tableId, $table['id']);
		$this->assertEquals($tableName, $table['name']);
		$this->assertNotEmpty($table['created']);
		$this->assertNotEmpty($table['lastChangeDate']);
		$this->assertNotEmpty($table['lastImportDate']);
		$this->assertEquals($expectationFileCsv->getHeader(), $table['columns']);
		$this->assertEmpty($table['indexedColumns']);
		$this->assertNotEquals('0000-00-00 00:00:00', $table['created']);
		$this->assertNotEmpty($table['dataSizeBytes']);

		$this->assertLinesEqualsSorted(
			file_get_contents($expectationFile),
			$this->_client->exportTable($tableId), 'initial data imported into table'
		);
	}

	public function tableCreateData()
	{
		return array(
			array('Languages', __DIR__ . '/../_data/languages.csv', __DIR__ . '/../_data/languages.csv', false, self::BACKEND_REDSHIFT),
			array('Languages', __DIR__ . '/../_data/languages.csv', __DIR__ . '/../_data/languages.csv', false, self::BACKEND_MYSQL),
			array('Languages', 'https://s3.amazonaws.com/keboola-tests/languages.csv', __DIR__ . '/../_data/languages.csv', false, self::BACKEND_MYSQL),
			array('Languages', 'https://s3.amazonaws.com/keboola-tests/languages.csv', __DIR__ . '/../_data/languages.csv', true, self::BACKEND_MYSQL),
			array('Languages', 'https://s3.amazonaws.com/keboola-tests/languages.csv', __DIR__ . '/../_data/languages.csv', false, self::BACKEND_REDSHIFT),
			array('Languages', 'https://s3.amazonaws.com/keboola-tests/languages.csv', __DIR__ . '/../_data/languages.csv', true, self::BACKEND_REDSHIFT),
			array('Languages', __DIR__ . '/../_data/languages.csv.gz', __DIR__ . '/../_data/languages.csv', false, self::BACKEND_MYSQL),
			array('Languages', __DIR__ . '/../_data/languages.csv.gz', __DIR__ . '/../_data/languages.csv', true, self::BACKEND_MYSQL),
			array('Languages', __DIR__ . '/../_data/languages.csv.gz', __DIR__ . '/../_data/languages.csv', true, self::BACKEND_REDSHIFT),
			array('Languages', __DIR__ . '/../_data/languages.csv', __DIR__ . '/../_data/languages.csv', true, self::BACKEND_MYSQL),
			array('Languages', __DIR__ . '/../_data/languages.csv', __DIR__ . '/../_data/languages.csv', true, self::BACKEND_REDSHIFT),
			array('Languages', __DIR__ . '/../_data/languages.camel-case-columns.csv', __DIR__ . '/../_data/languages.camel-case-columns.csv', true, self::BACKEND_MYSQL),
			array('Languages', __DIR__ . '/../_data/languages.camel-case-columns.csv', __DIR__ . '/../_data/languages.camel-case-columns.csv', true, self::BACKEND_REDSHIFT),
			array('Languages', __DIR__ . '/../_data/languages.camel-case-columns.csv', __DIR__ . '/../_data/languages.camel-case-columns.csv', false, self::BACKEND_REDSHIFT),
			array('Languages', __DIR__ . '/../_data/languages.camel-case-columns.csv', __DIR__ . '/../_data/languages.camel-case-columns.csv', false, self::BACKEND_MYSQL),

			// only numeric table and column names
			array('1', __DIR__ . '/../_data/numbers.csv', __DIR__ . '/../_data/numbers.csv', false, self::BACKEND_REDSHIFT),
			array('1', __DIR__ . '/../_data/numbers.csv', __DIR__ . '/../_data/numbers.csv', false, self::BACKEND_MYSQL),
			array('1', __DIR__ . '/../_data/numbers.csv', __DIR__ . '/../_data/numbers.csv', true, self::BACKEND_REDSHIFT),
			array('1', __DIR__ . '/../_data/numbers.csv', __DIR__ . '/../_data/numbers.csv', true, self::BACKEND_MYSQL),
		);
	}

	public function testTableWithUnsupportedCharactersInNameShouldNotBeCreated()
	{
		try {
			$tableId = $this->_client->createTable(
				$this->getTestBucketId(),
				'languages.main',
				new CsvFile(__DIR__ . '/../_data/languages.csv')
			);
			$this->fail('Table with dot in name should not be created');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation', $e->getStringCode());
		}
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableWithEmptyColumnNamesShouldNotBeCreated($backend)
	{
		try {
			$this->_client->createTable(
				$this->getTestBucketId(self::STAGE_IN, $backend),
				'languages',
				new CsvFile(__DIR__ . '/../_data/languages.invalid-column-name.csv')
			);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
		}
	}

	/**
	 * @param $async
	 * @dataProvider tableColumnSanitizeData
	 */
	public function testTableColumnNamesSanitize($backend, $async)
	{
		$csv = new Keboola\Csv\CsvFile(__DIR__ . '/../_data/filtering.csv');

		$method = $async ? 'createTableAsync' : 'createTable';
		$tableId = $this->_client->{$method}(
			$this->getTestBucketId(self::STAGE_IN, $backend),
			'sanitize',
			$csv
		);

		$table = $this->_client->getTable($tableId);
		$this->assertEquals(array('with_spaces', 'scrscz', 'with_underscore'), $table['columns']);
		$writeMethod = $async ? 'writeTableAsync' : 'writeTable';
		$this->_client->{$writeMethod}($tableId, new Keboola\Csv\CsvFile(__DIR__ . '/../_data/filtering.csv'));
	}

	/**
	 * @param $backend
	 * @param $async
	 * @dataProvider tableColumnSanitizeData
	 */
	public function testTableWithLongColumnNamesShouldNotBeCreated($backend, $async)
	{
		try {
			$method = $async ? 'createTableAsync' : 'createTable';
			$this->_client->{$method}(
				$this->getTestBucketId(self::STAGE_IN, $backend),
				'languages',
				new CsvFile(__DIR__ . '/../_data/long-column-names.csv')
			);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.invalidColumnName', $e->getStringCode());
		}
	}

	/**
	 * @param $async
	 * @dataProvider syncAsyncData
	 */
	public function testTableWithLongPkShouldNotBeCreatedInMysql($async)
	{
		try {
			$method = $async ? 'createTableAsync' : 'createTable';
			$this->_client->{$method}(
				$this->getTestBucketId(self::STAGE_IN, self::BACKEND_MYSQL),
				'languages',
				new CsvFile(__DIR__ . '/../_data/multiple-columns-pk.csv'),
				array(
					'primaryKey' => 'Paid_Search_Engine_Account,Date,Paid_Search_Campaign,Paid_Search_Ad_ID,Site__DFA',
				)
			);
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.primaryKeyTooLong', $e->getStringCode());
		}

	}

	/**
	 * @param $async
	 * @dataProvider syncAsyncData
	 */
	public function testTableWithLongPkShouldNotBeCreatedInRedshift($async)
	{
		$method = $async ? 'createTableAsync' : 'createTable';
		$id = $this->_client->{$method}(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT),
			'languages',
			new CsvFile(__DIR__ . '/../_data/multiple-columns-pk.csv'),
			array(
				'primaryKey' => 'Paid_Search_Engine_Account,Date,Paid_Search_Campaign,Paid_Search_Ad_ID,Site__DFA',
			)
		);
		$this->assertNotEmpty($id);
	}


	public function tableColumnSanitizeData()
	{
		return $this->dataWithBackendPrepended(array(
			array(false),
			array(true)
		));
	}

	public function syncAsyncData()
	{
		return array(
			array(false),
			array(true),
		);
	}

	public function testTableCreateWithPK()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$table = $this->_client->getTable($tableId);
		$this->assertEquals(array('id'), $table['primaryKey']);
		$this->assertEquals(array('id'), $table['indexedColumns']);
	}

	/**
	 * @dataProvider invalidPrimaryKeys
	 * @param $backend
	 */
	public function testTableCreateWithInvalidPK($backend, $primaryKey)
	{
		try {
			$this->_client->createTable(
				$this->getTestBucketId(self::STAGE_IN, $backend),
				'languages',
				new CsvFile(__DIR__ . '/../_data/languages.csv'),
				array(
					'primaryKey' => $primaryKey,
				)
			);
			$this->fail('exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.invalidPrimaryKeyColumns', $e->getStringCode());
		}

		try {
			$this->_client->createTableAsync(
				$this->getTestBucketId(self::STAGE_IN, $backend),
				'languages',
				new CsvFile(__DIR__ . '/../_data/languages.csv'),
				array(
					'primaryKey' => $primaryKey,
				)
			);
			$this->fail('exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.invalidPrimaryKeyColumns', $e->getStringCode());
		}
	}

	public function invalidPrimaryKeys()
	{
		return array(
			array(self::BACKEND_MYSQL, 'ID'),
			array(self::BACKEND_REDSHIFT, 'ID'),
			array(self::BACKEND_MYSQL, 'idus'),
			array(self::BACKEND_REDSHIFT, 'idus'),
		);
	}

}