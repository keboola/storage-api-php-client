<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 7/22/13
 * Time: 1:50 PM
 */

use Keboola\Csv\CsvFile;

class Keboola_StorageApi_SnapshottingTest extends StorageApiTestCase
{

	protected $_inBucketId;
	protected $_outBucketId;


	public function setUp()
	{
		parent::setUp();

		$this->_outBucketId = $this->_initEmptyBucket('api-tests', 'out');
		$this->_inBucketId = $this->_initEmptyBucket('api-tests', 'in');
	}


	public function testTableSnapshotCreate()
	{
		$tableId = $this->_client->createTable(
			$this->_inBucketId,
			'languages',
			new CsvFile(__DIR__ . '/_data/languages.csv'),
			array(
				'primaryKey' => 'id',
			)
		);
		$table = $this->_client->getTable($tableId);

		$description = 'Test snapshot';
		$snapshotId = $this->_client->createTableSnapshot($tableId, $description);
		$this->assertNotEmpty($snapshotId);

		$snapshot = $this->_client->getSnapshot($snapshotId);

		$this->assertEquals($description, $snapshot['description']);
		$this->assertEquals($table['primaryKey'], $snapshot['table']['primaryKey']);
		$this->assertEquals($table['columns'], $snapshot['table']['columns']);
	}

}