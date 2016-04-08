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

class Keboola_StorageApi_Tables_ImportExportCommonTest extends StorageApiTestCase
{


	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
	}

	/**
	 * @dataProvider tableImportData
	 * @param $importFileName
	 */
	public function testTableImportExport($backend, CsvFile $importFile, $expectationsFileName, $colNames, $format = 'rfc')
	{
		$expectationsFile = __DIR__ . '/../_data/' . $expectationsFileName;
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages-2', $importFile);

		$result = $this->_client->writeTable($tableId, $importFile);
		$table = $this->_client->getTable($tableId);

		$this->assertEmpty($result['warnings']);
		$this->assertEquals($colNames, array_values($result['importedColumns']), 'columns');
		$this->assertEmpty($result['transaction']);
		$this->assertNotEmpty($table['dataSizeBytes']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);

		// compare data
		$this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->exportTable($tableId, null, array(
			'format' => $format,
		)), 'imported data comparsion');

		// incremental
		$result = $this->_client->writeTable($tableId,  $importFile, array(
			'incremental' => true,
		));
		$this->assertNotEmpty($result['totalDataSizeBytes']);
	}

	/**
	 * @dataProvider tableImportData
	 * @param $importFileName
	 */
	public function testTableAsyncImportExport($backend, CsvFile $importFile, $expectationsFileName, $colNames, $format = 'rfc', $createTableOptions = array())
	{
		$expectationsFile = __DIR__ . '/../_data/' . $expectationsFileName;
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages-3', $importFile, $createTableOptions);

		$result = $this->_client->writeTableAsync($tableId, $importFile);
		$table = $this->_client->getTable($tableId);

		$this->assertEmpty($result['warnings']);
		$this->assertEquals($colNames, array_values($result['importedColumns']), 'columns');
		$this->assertEmpty($result['transaction']);
		$this->assertNotEmpty($table['dataSizeBytes']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);

		// compare data
		$this->assertLinesEqualsSorted(file_get_contents($expectationsFile), $this->_client->exportTable($tableId, null, array(
			'format' => $format,
		)), 'imported data comparsion');

		// incremental

		$result = $this->_client->writeTableAsync($tableId, $importFile, array(
			'incremental' => true,
		));
		$this->assertNotEmpty($result['totalDataSizeBytes']);
	}


	public function tableImportData()
	{
		return array(
			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/languages.csv'), 'languages.csv', array('id', 'name')),
			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/languages.csv'), 'languages.csv', array('id', 'name')),
			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/languages.csv'), 'languages.csv', array('id', 'name'), 'rfc', array(
				'primaryKey' => 'id,name',
			)),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/languages.special-column-names.csv'), 'languages.special-column-names.csv', array('Id', 'queryId'), 'rfc', array(
				'primaryKey' => 'Id,queryId',
			)),

			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/languages.special-column-names.csv'), 'languages.special-column-names.csv', array('Id', 'queryId'), 'rfc', array(
				'primaryKey' => 'Id,queryId',
			)),

			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv'), 'languages.csv', array('id', 'name')),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv'), 'languages.csv', array('id', 'name')),

			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('id', 'name')),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('id', 'name')),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/languages.utf8.bom.csv'), 'languages.csv', array('id', 'name')),
			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/languages.utf8.bom.csv'), 'languages.csv', array('id', 'name')),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/languages.csv.gz'), 'languages.csv', array('id', 'name')),
			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/languages.csv.gz'), 'languages.csv', array('id', 'name')),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),
			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/escaping.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.win.csv'), 'escaping.raw.win.csv', array('col1', 'col2_with_space'), 'raw'),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.raw.win.csv', "\t", "", "\\"), 'escaping.win.csv', array('col1', 'col2_with_space'), 'rfc'),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.nl-last-row.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),
			array(self::BACKEND_REDSHIFT, new CsvFile(__DIR__ . '/../_data/escaping.nl-last-row.csv'), 'escaping.standard.out.csv', array('col1', 'col2_with_space')),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.csv'), 'escaping.backslash.out.csv', array('col1', 'col2_with_space'), 'escaped'),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.csv'), 'escaping.raw.csv', array('col1', 'col2_with_space'), 'raw'),

			array(self::BACKEND_MYSQL, new CsvFile(__DIR__ . '/../_data/escaping.raw.csv', "\t", "", "\\"), 'escaping.raw.csv', array('col1', 'col2_with_space'), 'raw'),
		);
	}


	public function testImportWithWarnings()
	{
		$importFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, self::BACKEND_MYSQL), 'languages', $importFile);

		$warningsFile = new CsvFile(__DIR__ . '/../_data/warnings.languages.csv');
		$result = $this->_client->writeTable($tableId, $warningsFile);

		$this->assertCount(2, reset($result['warnings']));

		$this->assertLinesEqualsSorted(file_get_contents(__DIR__ . '/../_data/warnings.languages-export.csv'), $this->_client->exportTable($tableId), 'imported data comparsion');
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableImportColumnsCaseInsensitive($backend)
	{
		$importFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $importFile);

		$result = $this->_client->writeTableAsync($tableId, new CsvFile(__DIR__ . '/../_data/languages.camel-case-columns.csv'));

		$table = $this->_client->getTable($tableId);
		$this->assertEquals($importFile->getHeader(), $table['columns']);
	}


	/**
	 * @dataProvider tableImportInvalidData
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableInvalidImport($languagesFile)
	{
		$importCsvFile = new CsvFile(__DIR__ . '/../_data/' . $languagesFile);
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));

		$this->_client->writeTable($tableId, $importCsvFile);
	}

	public function tableImportInvalidData()
	{
		return array(
			array('languages.invalid.csv'),
			array('languages.invalid.gzip'),
			array('languages.invalid.zip'),
			array('languages.invalid.duplicateColumns.csv'),
		);
	}

	public function testTableImportNotExistingFile()
	{
		try {
			$this->_client->writeTable($this->getTestBucketId() . '.languages', new CsvFile('invalid.csv'));
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('fileNotReadable', $e->getStringCode());
		}
	}

	public function testTableImportInvalidLineBreaks()
	{
		$importCsvFile = new CsvFile(__DIR__ . '/../_data/escaping.mac-os-9.csv');
		try {
			$this->_client->createTable($this->getTestBucketId(), 'languages', $importCsvFile);
			$this->fail('Mac os 9 line breaks should not be allowd');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.validation.invalidParam', $e->getStringCode());
		}

		try {
			$this->_client->createTableAsync($this->getTestBucketId(), 'languages', $importCsvFile);
			$this->fail('Mac os 9 line breaks should not be allowd');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.validation.invalidParam', $e->getStringCode());
		}

		$createCsvFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $createCsvFile);
		try {
			$this->_client->writeTable($tableId, $importCsvFile);
			$this->fail('Mac os 9 line breaks should not be allowd');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.validation.invalidParam', $e->getStringCode());
		}
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableImportInvalidCsvParams($backend)
	{
		try {
			$this->_client->apiPost("storage/buckets/{$this->getTestBucketId(self::STAGE_IN, $backend)}/tables", [
				'dataString' => 'id,name',
				'delimiter' => '/t',
			]);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('invalidCsv', $e->getStringCode());
		}

		$fileId = $this->_client->uploadFile(__DIR__ . '/../_data/languages.csv', (new \Keboola\StorageApi\Options\FileUploadOptions())->setFileName('test.csv'));
		try {
			$this->_client->apiPost("storage/buckets/{$this->getTestBucketId(self::STAGE_IN, $backend)}/tables-async", [
				'dataFileId' => $fileId,
				'delimiter' => '/t',
			]);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('invalidCsv', $e->getStringCode());
		}

		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		try {
			$this->_client->apiPost("storage/tables/{$tableId}/import", [
				'dataString' => 'id,name',
				'delimiter' => '/t',
			]);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('invalidCsv', $e->getStringCode());
		}

		try {
			$this->_client->apiPost("storage/tables/{$tableId}/import-async", [
				'dataFileId' => $fileId,
				'delimiter' => '/t',
				'incremental' => true,
			]);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('invalidCsv', $e->getStringCode());
		}
	}



	/**
	 * @expectedException Keboola\StorageApi\ClientException
	 */
	public function testTableNotExistsImport()
	{
		$importCsvFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$this->_client->writeTable('languages', $importCsvFile);
	}


	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableImportCreateMissingColumns($backend)
	{
		$filePath = __DIR__ . '/../_data/languages.camel-case-columns.csv';
		$importFile = new CsvFile($filePath);
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $importFile);

		$extendedFile = __DIR__ . '/../_data/languages-more-columns.csv';
		$result = $this->_client->writeTable($tableId, new CsvFile($extendedFile));
		$table = $this->_client->getTable($tableId);

		$this->assertEmpty($result['warnings']);
		$this->assertEquals(array('Id','Name','iso','Something'), array_values($result['importedColumns']), 'columns');
		$this->assertEmpty($result['transaction']);
		$this->assertNotEmpty($table['dataSizeBytes']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);

		// compare data
		$this->assertLinesEqualsSorted(file_get_contents($extendedFile), $this->_client->exportTable($tableId, null, array(
			'format' => 'rfc',
		)), 'imported data comparsion');
	}


	public function testTableAsyncImportMissingFile()
	{
		$filePath = __DIR__ . '/../_data/languages.csv';
		$importFile = new CsvFile($filePath);
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);

		// prepare file but not upload it
		$file = $this->_client->prepareFileUpload((new \Keboola\StorageApi\Options\FileUploadOptions())->setFileName('languages.csv'));

		try {
			$this->_client->writeTableAsyncDirect($tableId, array(
				'dataFileId' => $file['id'],
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.fileNotUploaded', $e->getStringCode());
		}
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testImportWithoutHeaders($backend)
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new Keboola\Csv\CsvFile(__DIR__ . '/../_data/languages-headers.csv'));

		$importedFile = __DIR__ . '/../_data/languages-without-headers.csv';
		$result = $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), array(
			'withoutHeaders' => true,
		));
		$table = $this->_client->getTable($tableId);

		$this->assertEmpty($result['warnings']);
		$this->assertEmpty($result['transaction']);
		$this->assertNotEmpty($table['dataSizeBytes']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);
	}


	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testImportWithColumnsList($backend)
	{
		$headersCsv = new Keboola\Csv\CsvFile(__DIR__ . '/../_data/languages-headers.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $headersCsv);

		$importedFile = __DIR__ . '/../_data/languages-without-headers.csv';
		$result = $this->_client->writeTableAsync($tableId, new CsvFile($importedFile), array(
			'columns' => $headersCsv->getHeader(),
		));
		$table = $this->_client->getTable($tableId);

		$this->assertEmpty($result['warnings']);
		$this->assertEmpty($result['transaction']);
		$this->assertNotEmpty($table['dataSizeBytes']);
		$this->assertNotEmpty($result['totalDataSizeBytes']);
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableImportFromString($backend)
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', new Keboola\Csv\CsvFile(__DIR__ . '/../_data/languages-headers.csv'));

		$lines = '"id","name"';
		$lines .= "\n" . '"first","second"' . "\n";
		$this->_client->apiPost("storage/tables/$tableId/import", array(
			'dataString' => $lines,
		));

		$this->assertEquals($lines, $this->_client->exportTable($tableId, null, array(
			'format' =>'rfc',
		)));
	}

	public function testTableInvalidAsyncImport()
	{
		$importFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $importFile);
		$this->_client->addTableColumn($tableId, 'missing');
		try {
			$this->_client->writeTableAsync($tableId, $importFile);
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('csvImport.columnsNotMatch', $e->getStringCode());
			$this->arrayHasKey('exceptionId', $e->getContextParams());
			$this->assertArrayHasKey('job', $e->getContextParams());
			$job = $e->getContextParams()['job'];
			$this->assertEquals('error', $job['status']);
			$this->assertEquals(['missing'], $job['results']['missingColumns']);
			$this->assertEquals(['id', 'name', 'missing'], $job['results']['expectedColumns']);
		}
	}

	public function testTableInvalidPartialImport()
	{
		$createFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $createFile);
		$importFile = new CsvFile(__DIR__ . '/../_data/config.csv');
		try {
			$this->_client->writeTableAsync($tableId, $importFile, array(
				'partial' => true,
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('csvImport.noMatchingColumnsInTable', $e->getStringCode());
			$this->arrayHasKey('exceptionId', $e->getContextParams());
		}
	}

	public function testTableImportFromInvalidUrl()
	{
		$createFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $createFile);

		$csvFile = new CsvFile("http://unknown");
		try {
			$this->_client->writeTableAsync($tableId, $csvFile);
			$this->fail('Exception should be thrown on invalid URL');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.urlFetchError', $e->getStringCode());
		}

		try {
			$this->_client->writeTable($tableId, $csvFile);
			$this->fail('Exception should be thrown on invalid URL');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.urlFetchError', $e->getStringCode());
		}
	}

	public function testPartialImportMysql()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_MYSQL), 'users',
			new CsvFile(__DIR__ . '/../_data/users.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		$this->_client->writeTable($tableId, new CsvFile(__DIR__ . '/../_data/users-partial.csv'), array(
			'incremental' => true,
			'partial' => true,
		));

		$expectedData = Client::parseCsv(file_get_contents(__DIR__ . '/../_data/users-partial-expected.csv'), false);
		$parsedData = Client::parseCsv($this->_client->exportTable($tableId), false);

		$this->assertEquals($expectedData, $parsedData);
	}

	public function testPartialImportRedshift()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT), 'users',
			new CsvFile(__DIR__ . '/../_data/users.csv'),
			array(
				'primaryKey' => 'id',
			)
		);

		try {
			$this->_client->writeTable($tableId, new CsvFile(__DIR__ . '/../_data/users-partial.csv'), array(
				'incremental' => true,
				'partial' => true,
			));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.partialImportNotSupported', $e->getStringCode());
		}

	}

	public function testRedshiftErrorInCsv()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT), 'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);

		try {
			$this->_client->writeTableAsync($tableId, new Keboola\Csv\CsvFile(__DIR__ . '/../_data/languages.invalid-data.csv'));
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('invalidData', $e->getStringCode());
		}
	}

	/**
	 * Enclosure and escaped by should not be specified together
	 */
	public function testRedshiftUnsupportedCsvParams()
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT), 'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);

		$csv = new CsvFile(__DIR__ . '/../_data/languages.csv', ",", '"', "\\");
		try {
			$this->_client->writeTableAsync($tableId, $csv);
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('csvImport.invalidCsvParams', $e->getStringCode());
		}

		try {
			$this->_client->createTable(
				$this->getTestBucketId(self::STAGE_IN, self::BACKEND_REDSHIFT), 'languages-2',
				$csv
			);
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('csvImport.invalidCsvParams', $e->getStringCode());
		}
	}

	public function testEmptyTableAsyncExportShouldBeInFastQueue()
	{
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, self::BACKEND_MYSQL), 'languages', new CsvFile(__DIR__ . '/../_data/languages.csv'));
		$this->_client->deleteTableRows($tableId);

		$job = $this->_client->apiPost(
			"storage/tables/{$tableId}/export-async",
			null,
			$handleAsyncTask = false
		);
		$this->assertEquals('main_fast', $job['operationParams']['queue']);
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testTableImportFromEmptyFileShouldFail($backend)
	{
		$tableId = $this->_client->createTable(
			$this->getTestBucketId(self::STAGE_IN, $backend),
			'languages',
			new CsvFile(__DIR__ . '/../_data/languages.csv')
		);
		try {
			$this->_client->writeTable($tableId, new CsvFile(__DIR__ . '/../_data/empty.csv'));
			$this->fail('Table should not be imported');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('storage.tables.validation.noColumns', $e->getStringCode());
		}

		try {
			$fileId = $this->_client->uploadFile(__DIR__ . '/../_data/empty.csv', (new \Keboola\StorageApi\Options\FileUploadOptions())
					->setFileName('languages')
					->setCompress(false)
			);
			$this->_client->writeTableAsyncDirect(
				$tableId,
				[
					'dataFileId' => $fileId
				]
			);
			$this->fail('Table should not be imported');
		} catch (\Keboola\StorageApi\ClientException $e) {
			$this->assertEquals('csvImport.columnsNotMatch', $e->getStringCode());
		}
	}

	/**
	 * @dataProvider backends
	 * @param $importFileName
	 */
	public function testTableAsyncExportRepeatedly($backend)
	{
		$importFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
		$oldRunId = $this->_client->generateRunId();
		$this->_client->setRunId($oldRunId);

		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $importFile);
		$result = $this->_client->writeTable($tableId, $importFile);

		$this->assertEmpty($result['warnings']);

		// First export validation
		$fileId = $this->_client->exportTableAsync($tableId);
		$fileInfo = $this->_client->getFile($fileId["file"]["id"], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

		$this->assertArrayHasKey('runId', $fileInfo);
		$this->assertEquals($oldRunId, $fileInfo['runId']);

		$this->assertArrayHasKey('runIds', $fileInfo);
		$this->assertCount(1, $fileInfo['runIds']);

		$runIdExists = false;
		foreach ($fileInfo['runIds'] AS $runId) {
			if ($oldRunId == $runId) {
				$runIdExists = true;
			}
		}

		$this->assertTrue($runIdExists);

		// Second export validation (cached)
		$oldFileInfo = $fileInfo;
		$newRunId = $this->_client->generateRunId();
		$this->_client->setRunId($newRunId);

		$fileId = $this->_client->exportTableAsync($tableId);
		$fileInfo = $this->_client->getFile($fileId["file"]["id"], (new \Keboola\StorageApi\Options\GetFileOptions())->setFederationToken(true));

		$this->assertArrayHasKey('runId', $fileInfo);
		$this->assertEquals($oldRunId, $fileInfo['runId']);

		$this->assertArrayHasKey('runIds', $fileInfo);
		$this->assertCount(2, $fileInfo['runIds']);

		$runIdExists = false;
		foreach ($fileInfo['runIds'] AS $runId) {
			if ($newRunId == $runId) {
				$runIdExists = true;
			}
		}

		$this->assertTrue($runIdExists);


		$this->assertTrue($oldFileInfo["id"] === $fileInfo["id"]);
	}

	/**
	 * @dataProvider backends
	 * @param $backend
	 */
	public function testExtraColumnsImport($backend)
	{
		$bucketId = $this->getTestBucketId(self::STAGE_IN, $backend);
		if (!$this->_client->bucketExists($this->getTestBucketId())) {
			$this->_client->createBucket($this->getTestBucketId());
		}
		$tableId = $this->_client->createTable($bucketId, 'languages', new CsvFile(__DIR__ . '/../_data/languages-headers-incomplete.csv'));

		$fileId = $this->_client->uploadFile(__DIR__ . '/../_data/languages.csv', (new \Keboola\StorageApi\Options\FileUploadOptions())->setFileName('test.csv'));
		$this->_client->writeTableAsyncDirect($tableId, array(
			'dataFileId' => $fileId,
			'columns' => ['id']
		));
	}
}
