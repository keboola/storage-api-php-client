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

	public function testRedshiftTableDropWithViewShouldReturnDependencies()
	{
		$token = $this->_client->verifyToken();
		$dbh = $this->getDb($token);
		$dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

		$workingSchemaName = sprintf('tapi_%d_sand', $token['id']);
		$stmt = $dbh->prepare("SELECT * FROM pg_catalog.pg_namespace WHERE nspname = ?");
		$stmt->execute(array($workingSchemaName));
		$schema = $stmt->fetch();

		if (!$schema) {
			$dbh->query('CREATE SCHEMA ' . $workingSchemaName);
		}

		$stmt = $dbh->prepare("SELECT table_name FROM information_schema.views WHERE table_schema = ?");
		$stmt->execute(array($workingSchemaName));
		while ($table = $stmt->fetch()) {
			$dbh->query("DROP VIEW $workingSchemaName." . '"' . $table['table_name'] . '"');
		}

		$testBucketId = $this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT);
		$importFile = __DIR__ . '/../_data/languages.csv';
		$sourceTableId = $this->_client->createTable(
			$testBucketId,
			'languages',
			new CsvFile($importFile),
			array(
				'primaryKey' => 'id',
			)
		);
		$dbh->query("CREATE VIEW \"$workingSchemaName\".languages AS SELECT * FROM \"$testBucketId\".languages");

		try {
			$this->_client->dropTable($sourceTableId);
			$this->fail('Delete should not be allowed');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.dependentObjects', $e->getStringCode());
			$this->assertEquals([['id' => $token['id'], 'description' => $token['description']]], $e->getContextParams()['params']['dependencies']['sandbox']);
		}
		$dbh->query("DROP VIEW  \"$workingSchemaName\".languages");
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