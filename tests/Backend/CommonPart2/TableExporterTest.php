<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */


namespace Keboola\Test\Backend\CommonPart2;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\TableExporter;

class TableExporterTest extends StorageApiTestCase
{


    private $downloadPath;
    private $downloadPathGZip;

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        $this->downloadPath = __DIR__ . '/../../_tmp/languages.sliced.csv';
        $this->downloadPathGZip = __DIR__ . '/../../_tmp/languages.sliced.csv.gz';
    }

    /**
     * @dataProvider tableImportData
     * @param $importFileName
     */
    public function testTableAsyncExport(array $supportedBackends, CsvFile $importFile, $expectationsFileName, $exportOptions = array())
    {
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;
        $tokenData = $this->_client->verifyToken();
        if (!in_array($tokenData['owner']['defaultBackend'], $supportedBackends)) {
            return;
        }

        if (!isset($exportOptions['gzip'])) {
            $exportOptions['gzip'] = false;
        }

        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);
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

    public function testLimitParameter()
    {
        $importFile = new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $importFile);
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

            array([self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/1200.csv'), '1200.csv'),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv'),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.csv.gz'), 'languages.csv', array('gzip' => true)),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.csv'),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.two-cols.csv', array('columns' => array('0', '45'))),

            // tests the redshift data too long bug https://github.com/keboola/connection/issues/412
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.64k.csv'), 'languages.64k.csv'),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/languages.64k.csv'), 'languages.64k.csv',  array('gzip' => true)),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/64K.csv'), '64K.csv'),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT, self::BACKEND_SNOWFLAKE], new CsvFile('https://s3.amazonaws.com/keboola-tests/64K.csv'), '64K.csv',  array('gzip' => true)),

            array([self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.backslash.redshift.out.csv', array('format' => 'escaped')),
            array([self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.backslash.redshift.out.csv', array('format' => 'escaped')),
            array([self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.raw.redshift.out.csv', array('format' => 'raw')),
            array([self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.raw.redshift.out.csv', array('gzip' => true, 'format' => 'raw')),
            array([self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/escaping.csv'), 'escaping.standard.out.csv', array('gzip' => true)),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.csv', array('gzip' => true)),
            array([self::BACKEND_MYSQL, self::BACKEND_REDSHIFT], new CsvFile('https://s3.amazonaws.com/keboola-tests/numbers.csv'), 'numbers.two-cols.csv', array('gzip' => true, 'columns' => array('0', '45'))),
        );
    }
}
