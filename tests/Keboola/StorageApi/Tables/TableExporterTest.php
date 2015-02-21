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


	private $downloadPath;
	private $downloadPathGZip;

	public function setUp()
	{
		parent::setUp();
		$this->_initEmptyBucketsForAllBackends();
		$this->downloadPath = __DIR__ . '/../_tmp/languages.sliced.csv';
		$this->downloadPathGZip = __DIR__ . '/../_tmp/languages.sliced.csv.gz';
	}

	/**
	 * @dataProvider tableImportData
	 * @param $importFileName
	 */
	public function testTableAsyncExport($backend, CsvFile $importFile, $expectationsFileName, $exportOptions=array())
	{
		$expectationsFile = __DIR__ . '/../_data/' . $expectationsFileName;

		if (!isset($exportOptions['gzip']) ) {
			$exportOptions['gzip'] = false;
		}

		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $importFile);
		$result = $this->_client->writeTable($tableId, $importFile);

		$this->assertEmpty($result['warnings']);
		$exporter = new TableExporter($this->_client);

		if ($exportOptions['gzip'] === true) {
			$exporter->exportTable($tableId, $this->downloadPathGZip, $exportOptions);
			if (file_exists($this->downloadPath)) {
				unlink($this->downloadPath);
			}
			$process = new \Symfony\Component\Process\Process("gunzip " . escapeshellarg($this->downloadPathGZip));
			if (0 !== $process->run()) {
				throw new \Symfony\Component\Process\Exception\ProcessFailedException($process);
			}
		} else {
			$exporter->exportTable($tableId, $this->downloadPath, $exportOptions);
		}


		// compare data
		$this->assertTrue(file_exists($this->downloadPath));
		$this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparsion');

	}

	/**
	 * @param $backend
	 * @dataProvider backends
	 */
	public function testLimitParameter($backend)
	{
		$importFile = new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv');
		$tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN, $backend), 'languages', $importFile);
		$this->_client->writeTable($tableId, $importFile);

		$exportOptions = array(
			'limit' => 2,
		);
		$exporter = new TableExporter($this->_client);
		$exporter->exportTable($tableId, $this->downloadPath, $exportOptions);
		$this->assertTrue(file_exists($this->downloadPath));
		$parsed = Client::parseCsv(file_get_contents($this->downloadPath));
		$this->assertCount($exportOptions['limit'], $parsed);
	}

	public function tableImportData()
	{
		return array(
			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv'),
			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('gzip' => true)),
			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.csv'),
			array(self::BACKEND_MYSQL, new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.two-cols.csv', array('columns' => array('0', '45'))),

			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.backslash.redshift.out.csv', array('format' => 'escaped')),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.backslash.redshift.out.csv', array('format' => 'escaped')),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.raw.redshift.out.csv', array('format' => 'raw')),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.raw.redshift.out.csv', array('gzip' => true, 'format' => 'raw')),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.standard.out.csv', array('gzip' => true)),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.csv', array('gzip' => true)),
			array(self::BACKEND_REDSHIFT, new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.two-cols.csv', array('gzip' => true, 'columns' => array('0', '45'))),
		);
	}


}