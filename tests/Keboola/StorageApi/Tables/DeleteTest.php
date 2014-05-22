	<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_DeleteTest extends StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableDelete($backend)
	{
		$table1Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$table2Id = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages_2', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$tables = $this->_client->listTables($this->getTestBucketId(self::STAGE_IN, $backend));

		$this->assertCount(2, $tables);
		$this->_client->dropTable($table1Id);

		$tables = $this->_client->listTables($this->getTestBucketId(self::STAGE_IN, $backend));
		$this->assertCount(1, $tables);

		$table = reset($tables);
		$this->assertEquals($table2Id, $table['id']);
	}

}