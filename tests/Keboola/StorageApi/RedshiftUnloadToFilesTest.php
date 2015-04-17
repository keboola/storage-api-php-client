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

class Keboola_StorageApi_RedshiftUnloadToFilesTest extends StorageApiTestCase
{

	public function setUp()
	{
		parent::setUp();
	}

	public function encryptedData()
	{
		return [
			[false],
//			[true], - Redshift doesn't support manifest file encryption :(
		];
	}

	/**
	 * @dataProvider encryptedData
	 * @param $isEncrypted
	 */
	public function testUnload($isEncrypted)
	{
		$db = $this->initDb();

		$token = $this->_client->verifyToken();
		$workingSchemaName = sprintf('tapi_%d_tran', $token['id']);

		$uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
		$uploadOptions
			->setFileName('test.txt')
			->setIsSliced(true)
			->setNotify(false)
			->setIsEncrypted($isEncrypted);

		$slicedFile = $this->_client->prepareFileUpload($uploadOptions);
		$uploadParams = $slicedFile['uploadParams'];


		$query = "UNLOAD ('SELECT * FROM $workingSchemaName.\"out.languages\"') "
			. "TO 's3://" . $uploadParams["bucket"] . "/". $uploadParams["key"] . "' "
			. "CREDENTIALS 'aws_access_key_id=" . $uploadParams["credentials"]["AccessKeyId"] . ";aws_secret_access_key=" . $uploadParams["credentials"]["SecretAccessKey"] . ";token=" . $uploadParams["credentials"]["SessionToken"] . "' ";
		$query .= "DELIMITER ',' ADDQUOTES GZIP MANIFEST ";

		$db->query($query);
	}

	/**
	 * @return PDO
	 */
	private function initDb()
	{
		$token = $this->_client->verifyToken();
		$dbh = $this->getDb($token);
		$dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

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

		return $dbh;
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