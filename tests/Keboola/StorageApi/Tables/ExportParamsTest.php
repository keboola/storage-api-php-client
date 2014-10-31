<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\StorageApi\Client,
	Keboola\Csv\CsvFile;

class Keboola_StorageApi_Tables_ExportParamsTest extends StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	public function testInvalidExportFormat()
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));

		try {
			$this->_client->exportTable($tableId, null, array(
				'format' => 'csv',
			));
			$this->fail('Should throw exception');
		} catch(\Keboola\StorageApi\Exception $e) {
			$this->assertEquals('storage.tables.validation.invalidFormat', $e->getStringCode());
		}
	}

	public function testTableFileExport()
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));

		$outputFile = __DIR__ . '/../_tmp/languagesExport.csv';
		if (file_exists($outputFile)) {
			unlink($outputFile);
		}

		// Full download
		$this->_client->exportTable($tableId, $outputFile);
		$this->assertFileEquals($importFile, $outputFile);
		unlink($outputFile);

		// Download with limit
		$this->_client->exportTable($tableId, $outputFile, array(
			'limit' => 1,
		));

		$this->assertEquals(exec("wc -l < " . escapeshellarg($outputFile)), "2");
		unlink($outputFile);
	}

	/**
	 * @param $backend
	 * @dataProvider backends
	 */
	public function testTableExportParams($backend)
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new CsvFile($importFile));

		$originalFileLinesCount = exec("wc -l <" . escapeshellarg($importFile));

		$data = $this->_client->exportTable($tableId);
		$this->assertEquals($originalFileLinesCount,  count(Client::parseCsv($data, false)));

		$data = $this->_client->exportTable($tableId, null, array(
			'limit' => 2,
		));
		$this->assertEquals(3, count(Client::parseCsv($data, false)), "limit parameter");

		sleep(10);
		$startTime = time();
		$importCsv = new \Keboola\Csv\CsvFile($importFile);
		$this->_client->writeTable($tableId, $importCsv, array(
			'incremental' => true,
		));
		$this->_client->writeTable($tableId, $importCsv, array(
			'incremental' => true,
		));
		$data = $this->_client->exportTable($tableId);
		$this->assertEquals((3 * ($originalFileLinesCount - 1)) + 1, count(Client::parseCsv($data ,false)), "lines count after incremental load");

		$data = $this->_client->exportTable($tableId, null, array(
			'changedSince' => sprintf('-%d second', ceil(time() - $startTime) + 5),
		));
		$this->assertEquals((2 * ($originalFileLinesCount - 1)) + 1, count(Client::parseCsv($data ,false)), "changedSince parameter");

		$data = $this->_client->exportTable($tableId, null, array(
			'changedUntil' => sprintf('-%d second', ceil(time() - $startTime) + 5),
		));
		$this->assertEquals($originalFileLinesCount, count(Client::parseCsv($data, false)), "changedUntil parameter");
	}

	/**
	 * @param $exportOptions
	 * @param $expectedResult
	 * @dataProvider tableExportFiltersData
	 */
	public function testTableExportFilters($exportOptions, $expectedResult)
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$data = $this->_client->exportTable($tableId, null, $exportOptions);
		$parsedData = Client::parseCsv($data, false);
		array_shift($parsedData); // remove header

		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
	}

	public function testTableExportFilterShouldFailOnNonIndexedColumn()
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));

		try {
			$this->_client->exportTable($tableId, null, array(
				'whereColumn' => 'city',
				'whereValues' => array('PRG'),
			));
		} catch(\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.columnNotIndexed', $e->getStringCode());
		}
	}

	public function testTableExportColumnsParam()
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));

		$data = $this->_client->exportTable($tableId, null, array(
			'columns' => array('id'),
		));
		$parsed = Client::parseCsv($data, false);
		$firstRow = reset($parsed);

		$this->assertCount(1, $firstRow);
		$this->assertArrayHasKey(0, $firstRow);
		$this->assertEquals("id", $firstRow[0]);
	}

	public function testTableExportAsyncColumnsParam()
	{
		$importFile =  __DIR__ . '/../_data/languages.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile($importFile));

		$results = $this->_client->exportTableAsync($tableId, array(
			'columns' => array('id'),
		));
		$file = $this->_client->getFile($results['file']['id']);
		$parsed = Client::parseCsv(file_get_contents($file['url']), false);
		$firstRow = reset($parsed);

		$this->assertCount(1, $firstRow);
		$this->assertArrayHasKey(0, $firstRow);
		$this->assertEquals("id", $firstRow[0]);
	}

	/**
	 * @param $exportOptions
	 * @param $expectedResult
	 * @dataProvider tableExportFiltersData
	 */
	public function testTableExportAsyncMysql($exportOptions, $expectedResult)
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$results = $this->_client->exportTableAsync($tableId, $exportOptions);

		$exportedFile = $this->_client->getFile($results['file']['id']);
		$parsedData = Client::parseCsv(file_get_contents($exportedFile['url']), false);
		array_shift($parsedData); // remove header

		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
	}

	/**
	 * @param $exportOptions
	 * @param $expectedResult
	 * @dataProvider tableExportFiltersData
	 */
	public function testTableExportAsyncRedshift($exportOptions, $expectedResult)
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$csvFile = new CsvFile($importFile);
		$tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT), 'users', $csvFile, array(
			'columns' => $csvFile->getHeader(),
		));

		$results = $this->_client->exportTableAsync($tableId, array_merge($exportOptions, array(
			'format' => 'raw',
		)));

		$exportedFile = $this->_client->getFile($results['file']['id'], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

		$this->assertTrue($exportedFile['isSliced']);
		$this->assertGreaterThan(0, $exportedFile['sizeBytes']);

		$manifest = json_decode(file_get_contents($exportedFile['url']), true);

		$downloadCredentials = new Aws\Common\Credentials\Credentials(
			$exportedFile['credentials']['AccessKeyId'],
			$exportedFile['credentials']['SecretAccessKey'],
			$exportedFile['credentials']['SessionToken']
		);

		$s3Client = \Aws\S3\S3Client::factory(array('credentials' => $downloadCredentials));
		$s3Client->registerStreamWrapper();

		$csv = "";
		foreach ($manifest['entries'] as $filePart) {
			$csv .= file_get_contents($filePart['url']);
		}

		$parsedData = Client::parseCsv($csv, false, "\t", "");
		$this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);

		// Check S3 ACL and listing bucket
		$s3Client = \Aws\S3\S3Client::factory(array(
			"key" => $exportedFile["credentials"]["AccessKeyId"],
			"secret" => $exportedFile["credentials"]["SecretAccessKey"],
			"token" => $exportedFile["credentials"]["SessionToken"]
		));
		$bucket = $exportedFile["s3Path"]["bucket"];
		$prefix = $exportedFile["s3Path"]["key"];
		$objects = $s3Client->listObjects(array(
			"Bucket" => $bucket,
			"Prefix" => $prefix
		));
		$this->assertEquals(3, count($objects["Contents"]));
		foreach($objects["Contents"] as $object) {
			$this->assertStringStartsWith($prefix, $object["Key"]);
		}
	}

	public function testTableExportAsyncCache()
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$results = $this->_client->exportTableAsync($tableId);
		$fileId = $results['file']['id'];
		$this->assertFalse($results['cacheHit']);

		$results = $this->_client->exportTableAsync($tableId);
		$this->assertTrue($results['cacheHit']);
		$this->assertEquals($fileId, $results['file']['id']);

		$results = $this->_client->exportTableAsync($tableId, array(
			'gzip' => true,
		));

		$gzippedFileId = $results['file']['id'];
		$this->assertFalse($results['cacheHit']);
		$this->assertNotEquals($fileId, $gzippedFileId);
		$results = $this->_client->exportTableAsync($tableId, array(
			'gzip' => true,
		));
		$this->assertTrue($results['cacheHit']);
		$this->assertEquals($gzippedFileId, $results['file']['id']);

		$results = $this->_client->exportTableAsync($tableId, array(
			'whereColumn' => 'city',
			'whereValues' => array('PRG'),
		));
		$filteredByCityFileId = $results['file']['id'];
		$this->assertFalse($results['cacheHit']);
		$this->assertNotEquals($fileId, $filteredByCityFileId);

		$this->_client->writeTable($tableId, new CsvFile($importFile));

		$results = $this->_client->exportTableAsync($tableId);
		$newFileId = $results['file']['id'];
		$this->assertFalse($results['cacheHit']);
		$this->assertNotEquals($fileId, $newFileId);

		$results = $this->_client->exportTableAsync($tableId, array(
			'gzip' => true,
		));
		$this->assertFalse($results['cacheHit']);
	}

	/**
	 * Test access to cached file by various tokens
	 */
	public function testTableExportAsyncPermissions()
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$results = $this->_client->exportTableAsync($tableId);
		$fileId = $results['file']['id'];
		$this->assertFalse($results['cacheHit']);

		$newTokenId = $this->_client->createToken(array(
			$this->getTestBucketId() => 'read',
		));
		$newToken = $this->_client->getToken($newTokenId);
		$client = new Keboola\StorageApi\Client(array(
			'token' => $newToken['token'],
			'url' => STORAGE_API_URL,
		));

		$results = $client->exportTableAsync($tableId);
		$this->assertTrue($results['cacheHit']);
		$this->assertEquals($fileId, $results['file']['id']);


		$file = $client->getFile($results['file']['id']);
		Client::parseCsv(file_get_contents($file['url']), false);
	}

	public function testTableExportAsyncGzip()
	{
		$importFile =  __DIR__ . '/../_data/users.csv';
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'users', new CsvFile($importFile));
		$this->_client->markTableColumnAsIndexed($tableId, 'city');

		$results = $this->_client->exportTableAsync($tableId, array(
			'gzip' => true,
		));

		$exportedFile = $this->_client->getFile($results['file']['id']);
		$parsedData = Client::parseCsv(gzdecode(file_get_contents($exportedFile['url'])), false);
		array_shift($parsedData); // remove header

		$expected = Client::parseCsv(file_get_contents($importFile), false);
		array_shift($expected);

		$this->assertArrayEqualsSorted($expected, $parsedData, 0);
	}

}