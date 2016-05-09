<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Backend\Redshift;
use Keboola\Test\StorageApiTestCase;

class CopyImportTest extends StorageApiTestCase
{

	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyTestBuckets();
	}

	/**
	 * @param $schemaType
	 * @dataProvider schemaTyper
	 */
	public function testCopyCreate($schemaType)
	{
		$this->initDb($schemaType);
		$tableId = $this->_client->createTableAsyncDirect($this->getTestBucketId(self::STAGE_IN), array(
			'name' => 'languages',
			'dataTableName' => 'out.languages',
			'schemaType' => $schemaType,
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

	/**
	 * @param $schemaType
	 * @dataProvider schemaTyper
	 */
	public function testCopyImport($schemaType)
	{
		$this->initDb($schemaType);
		
		$table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
			'dataString' => 'Id,Name,update',
			'name' => 'languages',
			'primaryKey' => 'Id',
			'schemaType' => $schemaType,
		));

		$this->_client->writeTableAsyncDirect($table['id'], array(
			'dataTableName' => 'out.languages3',
			'schemaType' => $schemaType,
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

		$workingSchemaName = sprintf('tapi_%d_%s', $token['id'], ($schemaType == "luckyguess") ? 'luck' : 'tran');

		$db->query("truncate table $workingSchemaName.\"out.languages3\"");
		$db->query("insert into $workingSchemaName.\"out.languages3\" values (1, 'cz', '1'), (3, 'sk', '1');");

		$this->_client->writeTableAsyncDirect($table['id'], array(
			'dataTableName' => 'out.languages3',
			'incremental' => true,
			'schemaType' => $schemaType,
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
			'schemaType' => $schemaType,
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

	/**
	 * @param $schemaType
	 * @dataProvider schemaTyper
	 */
	public function testCopyImportFromNotExistingTableShouldReturnError($schemaType)
	{
		$this->initDb($schemaType);
		$table = $this->_client->apiPost("storage/buckets/" . $this->getTestBucketId(self::STAGE_IN) . "/tables", array(
			"dataString" => 'Id,Name,update',
			'name' => 'languages',
			'schemaType' => $schemaType,
		));

		try {
			$this->_client->writeTableAsyncDirect($table['id'], array(
				'dataTableName' => 'out.languagess',
				'schemaType' => $schemaType,
			));
			$this->fail('exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tableNotFound', $e->getStringCode());
		}
	}


	private function initDb($schemaType = "transformations")
	{
		$token = $this->_client->verifyToken();
		$dbh = $this->getDb($token);

		$workingSchemaName = sprintf('tapi_%d_%s', $token['id'], $schemaType == "luckyguess" ? 'luck' : 'tran');

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
	 * @return \PDO
	 */
	private function getDb($token)
	{
		return new \PDO(
			"pgsql:dbname={$token['owner']['redshift']['databaseName']};port=5439;host=" . REDSHIFT_HOSTNAME,
			REDSHIFT_USER,
			REDSHIFT_PASSWORD
		);
	}

	public function schemaTyper() {
		return array(
			array('transformations'),
			array('luckyguess'),
		);
	}
}