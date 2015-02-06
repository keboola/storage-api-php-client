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

	public function testCopyCreate()
	{
		$this->initDb();
		$tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT), array(
			'name' => 'languages',
			'dataTableName' => 'out.languages',
		));

		$expected = array(
			'"id","name"',
			'"1","cz"',
			'"2","en"',
		);

		$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($tableId, null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');
	}

	public function testCopyImport()
	{
		$this->initDb();
		$table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT) . "/tables", array(
			'dataString' => 'Id,Name,update',
			'name' => 'languages',
			'primaryKey' => 'Id',
		));

		$this->_client->writeTableAsyncDirect($table['id'], array(
			'dataTableName' => 'out.languages3',
		));

		$expected = array(
			'"Id","Name","update"',
			'"1","cz",""',
			'"2","en",""',
		);

		$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');

		$token = $this->_client->verifyToken();
		$db = $this->getDb($token);

		$workingSchemaName = sprintf('tapi_%d_tran', $token['id']);

		$db->query("truncate table $workingSchemaName.\"out.languages3\"");
		$db->query("insert into $workingSchemaName.\"out.languages3\" values (1, 'cz', '1'), (3, 'sk', '1');");

		$this->_client->writeTableAsyncDirect($table['id'], array(
			'dataTableName' => 'out.languages3',
			'incremental' => true,
		));

		$expected = array(
			'"Id","Name","update"',
			'"1","cz","1"',
			'"2","en",""',
			'"3","sk","1"',
		);
		$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
			'format' => 'rfc',
		)), 'previously null column updated');

		$db->query("truncate table $workingSchemaName.\"out.languages3\"");
		$db->query("alter table $workingSchemaName.\"out.languages3\" ADD COLUMN new_col varchar");
		$db->query("insert into $workingSchemaName.\"out.languages3\" values (1, 'cz', '1', null), (3, 'sk', '1', 'newValue');");

		$this->_client->writeTableAsyncDirect($table['id'], array(
			'dataTableName' => 'out.languages3',
			'incremental' => true,
		));

		$expected = array(
			'"Id","Name","update","new_col"',
			'"1","cz","1",""',
			'"2","en","",""',
			'"3","sk","1","newValue"',
		);
		$this->assertLinesEqualsSorted(implode("\n", $expected) . "\n", $this->_client->exportTable($table['id'], null, array(
			'format' => 'rfc',
		)), 'new  column added');
	}


	public function testCopyImportFromNotExistingTableShouldReturnError()
	{
		$this->initDb();
		$table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT) . "/tables", array(
			"dataString" => 'Id,Name',
			'name' => 'languages',
		));

		try {
			$this->_client->writeTableAsyncDirect($table['id'], array(
				'dataTableName' => 'out.languagess',
			));
			$this->fail('exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tableNotFound', $e->getStringCode());
		}
	}


	private function initDb()
	{
		$token = $this->_client->verifyToken();
		$dbh = $this->getDb($token);

		$workingSchemaName = sprintf('tapi_%d_tran', $token['id']);
		$stmt = $dbh->prepare("SELECT * FROM pg_catalog.pg_namespace WHERE nspname = ?");
		$stmt->execute(array($workingSchemaName));
		$schema = $stmt->fetch();

		if (!$schema) {
			$dbh->query('CREATE SCHEMA ' . $workingSchemaName);
		}

		$stmt = $dbh->prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = ?");
		$stmt->execute(array($workingSchemaName));
		while ($table = $stmt->fetch()) {
			$dbh->query("drop table $workingSchemaName." . '"' . $table['table_name'] . '"');
		}

		$dbh->query("create table $workingSchemaName.\"out.languages\" (
			Id integer not null,
			Name varchar(max) not null
		);");

		$dbh->query("insert into $workingSchemaName.\"out.languages\" values (1, 'cz'), (2, 'en');");

		$dbh->query("create table $workingSchemaName.\"out.languages2\" (
			Id integer,
			Name varchar(max)
		);");
		$dbh->query("insert into $workingSchemaName.\"out.languages2\" values (1, 'cz'), (NULL, 'en');");

		$dbh->query("create table $workingSchemaName.\"out.languages3\" (
			Id integer not null,
			Name varchar(max) not null,
			update varchar(10)
		);");

		$dbh->query("insert into $workingSchemaName.\"out.languages3\" values (1, 'cz'), (2, 'en');");
	}

	/**
	 * @return PDO
	 */
	private function getDb($token)
	{
		return new PDO(
			"pgsql:dbname={$token['owner']['redshift']['databaseName']};port=5439;host=" . REDSHIFT_HOSTNAME,
			REDSHIFT_USER,
			REDSHIFT_PASSWORD
		);
	}

}