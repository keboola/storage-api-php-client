<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

use Keboola\StorageApi\Client,
	Keboola\Csv\CsvFile,
	Keboola\StorageApi\TableExporter;

class Keboola_StorageApi_Tables_TableExporterTest extends StorageApiTestCase
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
	public function testTableAsyncExport($backend, CsvFile $importFile, $expectationsFileName, $exportOptions=array())
	{
		$downloadPath = __DIR__ . '/../_tmp/languages.csv';
		$expectationsFile = __DIR__ . '/../_data/' . $expectationsFileName;
		if (!isset($exportOptions['format'])) {
			$exportOptions['format'] = 'rfc';
		}
		if (isset($exportOptions['gzip']) && $exportOptions['gzip'] === true) {
			$downloadPath .= '.gz';
		} else {
			$exportOptions['gzip'] = false;
		}


		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $importFile);
		$result = $this->_client->writeTable($tableId, $importFile);

		$this->assertEmpty($result['warnings']);

		$exporter = new TableExporter($this->_client);
		$exporter->exportTable($tableId, $downloadPath, $exportOptions);

		if ($exportOptions['gzip'] === true) {
			(new \Symfony\Component\Process\Process("gunzip $downloadPath"))->mustRun();
			$downloadPath = substr($downloadPath, 0, -3);
		}
		// compare data
		$this->assertTrue(file_exists($downloadPath));
		$this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($downloadPath), 'imported data comparsion');

		if (file_exists($downloadPath)) {
			unlink($downloadPath);
		}

	}


	public function tableImportData()
	{
		return array(
			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv'),
			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('gzip' => true)),

			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv'),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('gzip' => true)),
		);
	}


}