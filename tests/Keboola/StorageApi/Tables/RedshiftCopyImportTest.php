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

class Keboola_StorageApi_Tables_RedshiftCopyImportTest extends StorageApiTestCase
{

	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	public function testCopyImport()
	{
		$this->initDb();
		$table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT) . "/tables", array(
			"dataString" => 'Id,Name',
			'name' => 'languages',
		));

		$result = $this->_client->writeTableAsyncDirect($table['id'], array(
			'dataTableName' => 'languages',
		));

		$expected = array(
			'"Id","Name"',
			'"1","cz"',
			'"2","en"',
		);

		$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');
	}

	private function initDb()
	{
		$token = $this->_client->verifyToken();

		$dbh = new PDO(
			"pgsql:dbname={$token['owner']['redshift']['databaseName']};port=5439;host=" . REDSHIFT_HOSTNAME,
			REDSHIFT_USER,
			REDSHIFT_PASSWORD
		);

		$workingSchemaName = sprintf('tapi_%d_tran', $token['id']);
		$stmt = $dbh->prepare("SELECT * FROM pg_catalog.pg_namespace WHERE nspname = ?");
		$stmt->execute(array($workingSchemaName));
		$schema = $stmt->fetch();

		if (!$schema) {
			$dbh->query('CREATE SCHEMA ' . $workingSchemaName);
		}

		$stmt = $dbh->prepare("SELECT table_name FROM information_schema.tables WHERE table_name = ? AND table_schema = ?");
		$stmt->execute(array('languages', $workingSchemaName));
		while ($table = $stmt->fetch()) {
			$dbh->query("drop table $workingSchemaName." . $table['table_name']);
		}

		$dbh->query("create table $workingSchemaName.languages (
			Id integer not null,
			Name varchar(max) not null
		);");

		$dbh->query("insert into $workingSchemaName.languages values (1, 'cz'), (2, 'en');");

	}

}